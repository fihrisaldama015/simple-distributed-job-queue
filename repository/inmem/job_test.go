package inmemrepo

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

func TestUpdateAppliesMutationAtomically(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepository()
	if err := repo.Save(ctx, newJob("job-1", "send-email", entity.StatusPending, time.Now())); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	updated, err := repo.Update(ctx, "job-1", func(j *entity.Job) error {
		j.Status = entity.StatusRunning
		j.Attempts++
		return nil
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated.Status != entity.StatusRunning || updated.Attempts != 1 {
		t.Fatalf("Update() returned %+v, want running/1", updated)
	}

	stored, err := repo.FindByID(ctx, "job-1")
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if stored.Status != entity.StatusRunning || stored.Attempts != 1 {
		t.Fatalf("store holds %+v, want running/1", stored)
	}
}

// A mutate error is the compare-and-swap rejection path: nothing may be written.
func TestUpdateWritesNothingWhenMutateFails(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepository()
	if err := repo.Save(ctx, newJob("job-1", "send-email", entity.StatusPending, time.Now())); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	_, err := repo.Update(ctx, "job-1", func(j *entity.Job) error {
		j.Status = entity.StatusRunning // must be discarded
		return entity.ErrNotClaimable
	})
	if !errors.Is(err, entity.ErrNotClaimable) {
		t.Fatalf("Update() error = %v, want ErrNotClaimable", err)
	}

	stored, err := repo.FindByID(ctx, "job-1")
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if stored.Status != entity.StatusPending {
		t.Fatalf("rejected update leaked into the store: %+v", stored)
	}
}

func TestUpdateMissingJob(t *testing.T) {
	_, err := newTestRepository().Update(context.Background(), "nope", func(*entity.Job) error { return nil })
	if !errors.Is(err, entity.ErrJobNotFound) {
		t.Fatalf("Update() error = %v, want ErrJobNotFound", err)
	}
}

// Concurrent increments through Update must not lose writes.
func TestUpdateIsAtomicUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepository()
	if err := repo.Save(ctx, newJob("job-1", "counter", entity.StatusPending, time.Now())); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	const increments = 200
	var wg sync.WaitGroup
	wg.Add(increments)
	for i := 0; i < increments; i++ {
		go func() {
			defer wg.Done()
			_, _ = repo.Update(ctx, "job-1", func(j *entity.Job) error {
				j.Attempts++
				return nil
			})
		}()
	}
	wg.Wait()

	stored, err := repo.FindByID(ctx, "job-1")
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if stored.Attempts != increments {
		t.Fatalf("Attempts = %d, want %d — updates were lost", stored.Attempts, increments)
	}
}

func TestSaveIfAbsentCreatesThenReturnsExisting(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepository()

	first, created, err := repo.SaveIfAbsent(ctx, "key-1", newJob("job-1", "send-email", entity.StatusPending, time.Now()))
	if err != nil {
		t.Fatalf("SaveIfAbsent() error = %v", err)
	}
	if !created {
		t.Fatal("SaveIfAbsent() created = false on first call, want true")
	}

	second, created, err := repo.SaveIfAbsent(ctx, "key-1", newJob("job-2", "send-email", entity.StatusPending, time.Now()))
	if err != nil {
		t.Fatalf("SaveIfAbsent() error = %v", err)
	}
	if created {
		t.Fatal("SaveIfAbsent() created = true on second call, want false")
	}
	if second.ID != first.ID {
		t.Fatalf("second call returned id %q, want the original %q", second.ID, first.ID)
	}

	all, err := repo.FindAll(ctx)
	if err != nil {
		t.Fatalf("FindAll() error = %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("store holds %d jobs, want 1", len(all))
	}
}

func TestSaveIfAbsentRejectsEmptyKey(t *testing.T) {
	_, _, err := newTestRepository().SaveIfAbsent(context.Background(), "",
		newJob("job-1", "task", entity.StatusPending, time.Now()))
	if !errors.Is(err, entity.ErrInvalidIdempotencyKey) {
		t.Fatalf("SaveIfAbsent() error = %v, want ErrInvalidIdempotencyKey", err)
	}
}

// The heart of the idempotency guarantee: a stampede on one key creates one job.
func TestSaveIfAbsentConcurrentSameKeyCreatesOnce(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepository()

	const callers = 100
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		creates int
		ids     = make(map[string]struct{})
	)

	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer wg.Done()
			job := newJob(fmt.Sprintf("job-%d", i), "send-email", entity.StatusPending, time.Now())
			stored, created, err := repo.SaveIfAbsent(ctx, "same-key", job)
			if err != nil {
				t.Errorf("SaveIfAbsent() error = %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if created {
				creates++
			}
			ids[stored.ID] = struct{}{}
		}(i)
	}
	wg.Wait()

	if creates != 1 {
		t.Fatalf("created %d jobs for one key, want exactly 1", creates)
	}
	if len(ids) != 1 {
		t.Fatalf("callers saw %d distinct job ids, want 1", len(ids))
	}
}

func TestCountByStatus(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepository()
	now := time.Now()

	statuses := []entity.Status{
		entity.StatusPending,
		entity.StatusRunning, entity.StatusRunning,
		entity.StatusFailed,
		entity.StatusCompleted, entity.StatusCompleted, entity.StatusCompleted,
	}
	for i, status := range statuses {
		if err := repo.Save(ctx, newJob(fmt.Sprintf("job-%d", i), "task", status, now)); err != nil {
			t.Fatalf("Save() error = %v", err)
		}
	}

	got, err := repo.CountByStatus(ctx)
	if err != nil {
		t.Fatalf("CountByStatus() error = %v", err)
	}
	want := entity.JobStatus{Pending: 1, Running: 2, Failed: 1, Completed: 3}
	if got != want {
		t.Fatalf("CountByStatus() = %+v, want %+v", got, want)
	}
	if got.Total() != int32(len(statuses)) {
		t.Fatalf("Total() = %d, want %d", got.Total(), len(statuses))
	}
}

// TestConcurrentAccessIsRaceFree exercises every method simultaneously. Run with
// -race, this is the proof that no caller ever shares memory with the store.
func TestConcurrentAccessIsRaceFree(t *testing.T) {
	ctx := context.Background()
	repo := newTestRepository()
	now := time.Now()

	const jobs = 50
	ids := make([]string, 0, jobs)
	for i := 0; i < jobs; i++ {
		id := fmt.Sprintf("job-%02d", i)
		ids = append(ids, id)
		if err := repo.Save(ctx, newJob(id, "task", entity.StatusPending, now.Add(time.Duration(i)*time.Millisecond))); err != nil {
			t.Fatalf("Save() error = %v", err)
		}
	}

	var wg sync.WaitGroup
	const rounds = 20

	// Writers: transition jobs through the state machine.
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				_, _ = repo.Update(ctx, id, func(j *entity.Job) error {
					j.Attempts++
					j.Status = entity.StatusRunning
					j.UpdatedAt = time.Now()
					return nil
				})
				_, _ = repo.Update(ctx, id, func(j *entity.Job) error {
					j.Status = entity.StatusCompleted
					return nil
				})
			}
		}(id)
	}

	// Readers: everything the dashboard and GraphQL layer do.
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				if _, err := repo.FindAll(ctx); err != nil {
					t.Errorf("FindAll() error = %v", err)
				}
				if _, err := repo.CountByStatus(ctx); err != nil {
					t.Errorf("CountByStatus() error = %v", err)
				}
				if _, err := repo.FindByIDs(ctx, ids); err != nil {
					t.Errorf("FindByIDs() error = %v", err)
				}
				if job, err := repo.FindByID(ctx, ids[i%len(ids)]); err == nil {
					job.Status = entity.StatusFailed // mutating our copy must be harmless
				}
			}
		}(i)
	}

	// Idempotent creators racing on a shared key.
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, _ = repo.SaveIfAbsent(ctx, "shared-key",
				newJob(fmt.Sprintf("extra-%02d", i), "task", entity.StatusPending, now))
		}(i)
	}

	wg.Wait()

	counts, err := repo.CountByStatus(ctx)
	if err != nil {
		t.Fatalf("CountByStatus() error = %v", err)
	}
	all, err := repo.FindAll(ctx)
	if err != nil {
		t.Fatalf("FindAll() error = %v", err)
	}
	if counts.Total() != int32(len(all)) {
		t.Fatalf("counts total %d but store holds %d jobs — accounting drifted", counts.Total(), len(all))
	}
	if len(all) != jobs+1 {
		t.Fatalf("store holds %d jobs, want %d (%d seeded + 1 from the shared key)", len(all), jobs+1, jobs)
	}
}
