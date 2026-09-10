package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"jobqueue/entity"
	_interface "jobqueue/interface"
	inmemrepo "jobqueue/repository/inmem"
)

type fakeDispatcher struct {
	mu  sync.Mutex
	ids []string
	err error
}

func (f *fakeDispatcher) Dispatch(ctx context.Context, jobID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.ids = append(f.ids, jobID)
	return nil
}

func (f *fakeDispatcher) dispatched() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ids...)
}

func newTestService(dispatcher _interface.JobDispatcher) (_interface.JobService, _interface.JobRepository) {
	repo := inmemrepo.NewJobRepository().
		SetInMemConnection(make(map[string]*entity.Job)).
		Build()

	svc := NewJobService().
		SetJobRepository(repo).
		SetJobDispatcher(dispatcher).
		SetMaxAttempts(3).
		Build()

	return svc, repo
}

func TestEnqueueRejectsEmptyTask(t *testing.T) {
	svc, _ := newTestService(&fakeDispatcher{})

	for _, task := range []string{"", "   ", "\t\n"} {
		if _, err := svc.Enqueue(context.Background(), task, ""); !errors.Is(err, entity.ErrInvalidTask) {
			t.Fatalf("Enqueue(%q) error = %v, want ErrInvalidTask", task, err)
		}
	}
}

func TestEnqueueRejectsOverlongTask(t *testing.T) {
	svc, _ := newTestService(&fakeDispatcher{})

	_, err := svc.Enqueue(context.Background(), strings.Repeat("x", 129), "")
	if !errors.Is(err, entity.ErrTaskNameTooLong) {
		t.Fatalf("Enqueue() error = %v, want ErrTaskNameTooLong", err)
	}
}

func TestEnqueuePersistsPendingJobAndDispatchesOnce(t *testing.T) {
	dispatcher := &fakeDispatcher{}
	svc, repo := newTestService(dispatcher)

	job, err := svc.Enqueue(context.Background(), "  send-email  ", "")
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if job.ID == "" {
		t.Fatal("Enqueue() returned a job without an id")
	}
	if job.Task != "send-email" {
		t.Fatalf("Task = %q, want the trimmed name", job.Task)
	}
	if job.Status != entity.StatusPending || job.Attempts != 0 {
		t.Fatalf("job = %+v, want pending with 0 attempts", job)
	}
	if job.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d, want 3", job.MaxAttempts)
	}

	stored, err := repo.FindByID(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if stored.Task != "send-email" {
		t.Fatalf("stored task = %q", stored.Task)
	}

	if got := dispatcher.dispatched(); len(got) != 1 || got[0] != job.ID {
		t.Fatalf("dispatched = %v, want exactly [%s]", got, job.ID)
	}
}

func TestEnqueueWithSameIdempotencyKeyReturnsSameJob(t *testing.T) {
	dispatcher := &fakeDispatcher{}
	svc, _ := newTestService(dispatcher)

	first, err := svc.Enqueue(context.Background(), "send-email", "order-42")
	if err != nil {
		t.Fatalf("first Enqueue() error = %v", err)
	}
	second, err := svc.Enqueue(context.Background(), "send-email", "order-42")
	if err != nil {
		t.Fatalf("second Enqueue() error = %v", err)
	}

	if second.ID != first.ID {
		t.Fatalf("second id = %q, want the first id %q", second.ID, first.ID)
	}
	if got := dispatcher.dispatched(); len(got) != 1 {
		t.Fatalf("dispatched %d times, want 1 — the duplicate must not be queued", len(got))
	}

	all, err := svc.GetAllJobs(context.Background())
	if err != nil {
		t.Fatalf("GetAllJobs() error = %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("store holds %d jobs, want 1", len(all))
	}
}

// A rejected dispatch must never leave a job pending with nobody to run it.
func TestEnqueueMarksJobFailedWhenDispatchFails(t *testing.T) {
	dispatcher := &fakeDispatcher{err: entity.ErrQueueFull}
	svc, repo := newTestService(dispatcher)

	_, err := svc.Enqueue(context.Background(), "send-email", "")
	if !errors.Is(err, entity.ErrQueueFull) {
		t.Fatalf("Enqueue() error = %v, want ErrQueueFull", err)
	}

	all, err := repo.FindAll(context.Background())
	if err != nil {
		t.Fatalf("FindAll() error = %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("store holds %d jobs, want 1", len(all))
	}
	if all[0].Status != entity.StatusFailed {
		t.Fatalf("Status = %q, want failed", all[0].Status)
	}
	if !strings.Contains(all[0].LastError, "full") {
		t.Fatalf("LastError = %q, want it to explain the rejection", all[0].LastError)
	}
}

func TestGetJobNotFound(t *testing.T) {
	svc, _ := newTestService(&fakeDispatcher{})

	if _, err := svc.GetJob(context.Background(), "nope"); !errors.Is(err, entity.ErrJobNotFound) {
		t.Fatalf("GetJob() error = %v, want ErrJobNotFound", err)
	}
}

// [Job!]! must never serialise as null.
func TestGetAllJobsReturnsEmptySliceNotNil(t *testing.T) {
	svc, _ := newTestService(&fakeDispatcher{})

	jobs, err := svc.GetAllJobs(context.Background())
	if err != nil {
		t.Fatalf("GetAllJobs() error = %v", err)
	}
	if jobs == nil {
		t.Fatal("GetAllJobs() returned nil, want an empty slice")
	}
}

// ListJobs backs the dashboard's status filter, task search and sort toggle - none of
// which exist anywhere else, so this is the only place their behavior is proven.
func TestListJobsFiltersByStatus(t *testing.T) {
	svc, repo := newTestService(&fakeDispatcher{})
	seedListJobs(t, repo)

	jobs, err := svc.ListJobs(context.Background(), _interface.JobListOptions{Status: entity.StatusRunning})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].Task != "send-email" {
		t.Fatalf("jobs = %+v, want exactly the one running job", jobs)
	}
}

func TestListJobsSearchesTaskCaseInsensitively(t *testing.T) {
	svc, repo := newTestService(&fakeDispatcher{})
	seedListJobs(t, repo)

	jobs, err := svc.ListJobs(context.Background(), _interface.JobListOptions{TaskQuery: "UNSTABLE"})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].Task != "unstable-job" {
		t.Fatalf("jobs = %+v, want exactly the unstable-job", jobs)
	}
}

func TestListJobsCombinesStatusAndTaskFilters(t *testing.T) {
	svc, repo := newTestService(&fakeDispatcher{})
	seedListJobs(t, repo)

	jobs, err := svc.ListJobs(context.Background(), _interface.JobListOptions{
		Status: entity.StatusCompleted, TaskQuery: "load",
	})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].Task != "load-test" {
		t.Fatalf("jobs = %+v, want exactly the completed load-test job", jobs)
	}
}

func TestListJobsSortsNewestFirstOnRequest(t *testing.T) {
	svc, repo := newTestService(&fakeDispatcher{})
	seedListJobs(t, repo)

	oldestFirst, err := svc.ListJobs(context.Background(), _interface.JobListOptions{})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	newestFirst, err := svc.ListJobs(context.Background(), _interface.JobListOptions{NewestFirst: true})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}

	if len(oldestFirst) != len(newestFirst) {
		t.Fatalf("oldestFirst has %d jobs, newestFirst has %d - a sort must not drop jobs", len(oldestFirst), len(newestFirst))
	}
	n := len(oldestFirst)
	for i := 0; i < n; i++ {
		if oldestFirst[i].ID != newestFirst[n-1-i].ID {
			t.Fatalf("newestFirst is not the exact reverse of oldestFirst at index %d", i)
		}
	}
}

// An empty JobListOptions must behave exactly like GetAllJobs - the dashboard calls
// this with no filters active on every ordinary poll.
func TestListJobsWithNoOptionsMatchesGetAllJobs(t *testing.T) {
	svc, repo := newTestService(&fakeDispatcher{})
	seedListJobs(t, repo)

	all, err := svc.GetAllJobs(context.Background())
	if err != nil {
		t.Fatalf("GetAllJobs() error = %v", err)
	}
	listed, err := svc.ListJobs(context.Background(), _interface.JobListOptions{})
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(all) != len(listed) {
		t.Fatalf("GetAllJobs returned %d, ListJobs returned %d", len(all), len(listed))
	}
	for i := range all {
		if all[i].ID != listed[i].ID {
			t.Fatalf("order differs at index %d: GetAllJobs=%s ListJobs=%s", i, all[i].ID, listed[i].ID)
		}
	}
}

// seedListJobs saves four jobs directly (bypassing Enqueue/dispatch) covering every
// status and a range of task names, so filter/search/sort tests have a fixed target.
func seedListJobs(t *testing.T, repo _interface.JobRepository) {
	t.Helper()
	now := time.Now()
	jobs := []*entity.Job{
		{ID: "job-1", Task: "send-email", Status: entity.StatusPending, CreatedAt: now},
		{ID: "job-2", Task: "send-email", Status: entity.StatusRunning, CreatedAt: now.Add(time.Second)},
		{ID: "job-3", Task: "unstable-job", Status: entity.StatusFailed, CreatedAt: now.Add(2 * time.Second)},
		{ID: "job-4", Task: "load-test", Status: entity.StatusCompleted, CreatedAt: now.Add(3 * time.Second)},
	}
	for _, job := range jobs {
		if err := repo.Save(context.Background(), job); err != nil {
			t.Fatalf("Save() error = %v", err)
		}
	}
}

func TestGetJobStatusCountsEnqueuedJobs(t *testing.T) {
	svc, _ := newTestService(&fakeDispatcher{})

	for i := 0; i < 3; i++ {
		if _, err := svc.Enqueue(context.Background(), "send-email", ""); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	counts, err := svc.GetJobStatus(context.Background())
	if err != nil {
		t.Fatalf("GetJobStatus() error = %v", err)
	}
	if counts.Pending != 3 || counts.Total() != 3 {
		t.Fatalf("counts = %+v, want 3 pending", counts)
	}
}
