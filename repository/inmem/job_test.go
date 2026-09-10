package inmemrepo

import (
	"context"
	"errors"
	"testing"
	"time"

	"jobqueue/entity"
	_interface "jobqueue/interface"
)

func newTestRepository() _interface.JobRepository {
	return NewJobRepository().
		SetInMemConnection(make(map[string]*entity.Job)).
		Build()
}

func newJob(id, task string, status entity.Status, createdAt time.Time) *entity.Job {
	return &entity.Job{
		ID:          id,
		Task:        task,
		Status:      status,
		MaxAttempts: 3,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}
}

// The caller keeps its own job struct after Save. Mutating it must not reach the store.
func TestSaveDoesNotAliasCallerJob(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepository()
	job := newJob("job-1", "send-email", entity.StatusPending, time.Now())

	if err := repo.Save(ctx, job); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	job.Status = entity.StatusCompleted
	job.Attempts = 42

	stored, err := repo.FindByID(ctx, "job-1")
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if stored.Status != entity.StatusPending || stored.Attempts != 0 {
		t.Fatalf("store aliased the caller's job: %+v", stored)
	}
}

// Symmetrically: the job handed to a reader must not alias the stored job.
func TestFindByIDReturnsIndependentCopy(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepository()
	if err := repo.Save(ctx, newJob("job-1", "send-email", entity.StatusPending, time.Now())); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	first, err := repo.FindByID(ctx, "job-1")
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	first.Status = entity.StatusFailed

	second, err := repo.FindByID(ctx, "job-1")
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if second.Status != entity.StatusPending {
		t.Fatalf("reader mutation leaked into the store: %+v", second)
	}
}

func TestFindByIDNotFound(t *testing.T) {
	_, err := newTestRepository().FindByID(context.Background(), "nope")
	if !errors.Is(err, entity.ErrJobNotFound) {
		t.Fatalf("FindByID() error = %v, want ErrJobNotFound", err)
	}
}

func TestFindByIDsReturnsOnlyExistingJobs(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepository()
	now := time.Now()
	for _, id := range []string{"job-1", "job-2"} {
		if err := repo.Save(ctx, newJob(id, "task", entity.StatusPending, now)); err != nil {
			t.Fatalf("Save() error = %v", err)
		}
	}

	got, err := repo.FindByIDs(ctx, []string{"job-1", "missing", "job-2"})
	if err != nil {
		t.Fatalf("FindByIDs() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("FindByIDs() returned %d jobs, want 2", len(got))
	}
	if got["job-1"] == nil || got["job-2"] == nil {
		t.Fatalf("FindByIDs() missing an existing job: %v", got)
	}
	if _, ok := got["missing"]; ok {
		t.Fatal("FindByIDs() invented an entry for a missing id")
	}
}

// Map iteration order is random; the dashboard table would reshuffle on every 2s poll.
func TestFindAllOrdersByCreatedAt(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepository()
	base := time.Now()

	// Saved out of order on purpose.
	for _, spec := range []struct {
		id     string
		offset time.Duration
	}{{"job-c", 2 * time.Second}, {"job-a", 0}, {"job-b", time.Second}} {
		if err := repo.Save(ctx, newJob(spec.id, "task", entity.StatusPending, base.Add(spec.offset))); err != nil {
			t.Fatalf("Save() error = %v", err)
		}
	}

	jobs, err := repo.FindAll(ctx)
	if err != nil {
		t.Fatalf("FindAll() error = %v", err)
	}
	want := []string{"job-a", "job-b", "job-c"}
	if len(jobs) != len(want) {
		t.Fatalf("FindAll() returned %d jobs, want %d", len(jobs), len(want))
	}
	for i, id := range want {
		if jobs[i].ID != id {
			t.Fatalf("FindAll()[%d].ID = %q, want %q", i, jobs[i].ID, id)
		}
	}
}

// [Job!]! must never serialise as null.
func TestFindAllOnEmptyStoreReturnsEmptySlice(t *testing.T) {
	jobs, err := newTestRepository().FindAll(context.Background())
	if err != nil {
		t.Fatalf("FindAll() error = %v", err)
	}
	if jobs == nil {
		t.Fatal("FindAll() returned nil, want an empty slice")
	}
	if len(jobs) != 0 {
		t.Fatalf("FindAll() returned %d jobs, want 0", len(jobs))
	}
}
