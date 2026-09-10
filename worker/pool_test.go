package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"jobqueue/entity"
	_interface "jobqueue/interface"
	inmemrepo "jobqueue/repository/inmem"

	"go.uber.org/zap"
)

// Composing the pool with the real repository (already covered by its own tests) makes
// these tests exercise the actual claim semantics rather than a fake's approximation.
func newTestPool(t *testing.T, tune func(*Config)) (*Pool, _interface.JobRepository, *Registry) {
	t.Helper()

	repo := inmemrepo.NewJobRepository().
		SetInMemConnection(make(map[string]*entity.Job)).
		Build()
	registry := NewRegistry(0)

	cfg := Config{
		Workers:       4,
		QueueSize:     32,
		MaxAttempts:   3,
		BaseBackoff:   time.Millisecond,
		MaxBackoff:    5 * time.Millisecond,
		ShutdownGrace: 5 * time.Second,
	}
	if tune != nil {
		tune(&cfg)
	}

	pool := New(cfg, repo, registry, zap.NewNop())
	pool.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pool.Shutdown(ctx)
	})

	return pool, repo, registry
}

func seedJob(t *testing.T, repo _interface.JobRepository, id, task string, maxAttempts int32) {
	t.Helper()
	job := &entity.Job{
		ID:          id,
		Task:        task,
		Status:      entity.StatusPending,
		MaxAttempts: maxAttempts,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if err := repo.Save(context.Background(), job); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

// waitForStatus polls instead of sleeping a fixed duration: fast when the system is
// fast, and it fails with a useful message instead of flaking.
func waitForStatus(t *testing.T, repo _interface.JobRepository, id string, want entity.Status, timeout time.Duration) *entity.Job {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last *entity.Job
	for time.Now().Before(deadline) {
		job, err := repo.FindByID(context.Background(), id)
		if err == nil {
			last = job
			if job.Status == want {
				return job
			}
		}
		time.Sleep(2 * time.Millisecond)
	}

	t.Fatalf("job %s did not reach %q within %s (last seen: %+v)", id, want, timeout, last)
	return nil
}

func TestPoolCompletesJob(t *testing.T) {
	pool, repo, registry := newTestPool(t, nil)
	registry.Register("send-email", func(context.Context, entity.Job) error { return nil })
	seedJob(t, repo, "job-1", "send-email", 3)

	if err := pool.Dispatch(context.Background(), "job-1"); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	job := waitForStatus(t, repo, "job-1", entity.StatusCompleted, 2*time.Second)
	if job.Attempts != 1 {
		t.Fatalf("Attempts = %d, want 1", job.Attempts)
	}
	if job.LastError != "" {
		t.Fatalf("LastError = %q, want empty", job.LastError)
	}
}

func TestPoolRetriesUntilMaxAttemptsThenFails(t *testing.T) {
	pool, repo, registry := newTestPool(t, nil)

	var calls int32
	var mu sync.Mutex
	registry.Register("always-fails", func(context.Context, entity.Job) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return errors.New("boom")
	})
	seedJob(t, repo, "job-1", "always-fails", 3)

	if err := pool.Dispatch(context.Background(), "job-1"); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	job := waitForStatus(t, repo, "job-1", entity.StatusFailed, 2*time.Second)
	if job.Attempts != 3 {
		t.Fatalf("Attempts = %d, want 3", job.Attempts)
	}
	if !strings.Contains(job.LastError, "boom") {
		t.Fatalf("LastError = %q, want it to contain the handler error", job.LastError)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 3 {
		t.Fatalf("handler called %d times, want 3", calls)
	}
}

// The assignment's headline scenario.
func TestPoolUnstableJobSucceedsOnThirdAttempt(t *testing.T) {
	pool, repo, registry := newTestPool(t, nil)
	registry.Register("unstable-job", UnstableHandler(2, 0))
	seedJob(t, repo, "job-1", "unstable-job", 3)

	if err := pool.Dispatch(context.Background(), "job-1"); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	job := waitForStatus(t, repo, "job-1", entity.StatusCompleted, 2*time.Second)
	if job.Attempts != 3 {
		t.Fatalf("Attempts = %d, want exactly 3", job.Attempts)
	}
}

// A panicking handler must not take the process down.
func TestPoolRecoversHandlerPanic(t *testing.T) {
	pool, repo, registry := newTestPool(t, nil)
	registry.Register("panics", func(context.Context, entity.Job) error {
		panic("handler exploded")
	})
	seedJob(t, repo, "job-1", "panics", 2)

	if err := pool.Dispatch(context.Background(), "job-1"); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	job := waitForStatus(t, repo, "job-1", entity.StatusFailed, 2*time.Second)
	if job.Attempts != 2 {
		t.Fatalf("Attempts = %d, want 2", job.Attempts)
	}
	if !strings.Contains(job.LastError, "panicked") {
		t.Fatalf("LastError = %q, want it to mention the panic", job.LastError)
	}

	// The pool must still be alive.
	registry.Register("healthy", func(context.Context, entity.Job) error { return nil })
	seedJob(t, repo, "job-2", "healthy", 1)
	if err := pool.Dispatch(context.Background(), "job-2"); err != nil {
		t.Fatalf("Dispatch() after a panic error = %v", err)
	}
	waitForStatus(t, repo, "job-2", entity.StatusCompleted, 2*time.Second)
}

// Idempotency: the same id delivered twice runs the handler once.
func TestPoolDuplicateDeliveryRunsHandlerOnce(t *testing.T) {
	pool, repo, registry := newTestPool(t, func(c *Config) { c.Workers = 4 })

	var (
		mu    sync.Mutex
		calls int
	)
	registry.Register("slow", func(context.Context, entity.Job) error {
		mu.Lock()
		calls++
		mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	seedJob(t, repo, "job-1", "slow", 3)

	for i := 0; i < 5; i++ {
		if err := pool.Dispatch(context.Background(), "job-1"); err != nil {
			t.Fatalf("Dispatch() error = %v", err)
		}
	}

	waitForStatus(t, repo, "job-1", entity.StatusCompleted, 2*time.Second)
	time.Sleep(100 * time.Millisecond) // let any stray duplicate try to run

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("handler called %d times for one job, want exactly 1", calls)
	}
}

func TestDispatchReturnsErrQueueFull(t *testing.T) {
	pool, repo, registry := newTestPool(t, func(c *Config) {
		c.Workers = 1
		c.QueueSize = 1
	})

	// Buffered: a second delivery must not block the worker on an unread channel,
	// which would make the cleanup Shutdown wait out its whole grace period.
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	registry.Register("blocks", func(context.Context, entity.Job) error {
		started <- struct{}{}
		<-release
		return nil
	})
	defer close(release)

	for i := 1; i <= 3; i++ {
		seedJob(t, repo, fmt.Sprintf("job-%d", i), "blocks", 1)
	}

	// job-1 occupies the single worker...
	if err := pool.Dispatch(context.Background(), "job-1"); err != nil {
		t.Fatalf("Dispatch(job-1) error = %v", err)
	}
	<-started

	// ...job-2 fills the single buffer slot...
	if err := pool.Dispatch(context.Background(), "job-2"); err != nil {
		t.Fatalf("Dispatch(job-2) error = %v", err)
	}

	// ...and job-3 has nowhere to go.
	if err := pool.Dispatch(context.Background(), "job-3"); !errors.Is(err, entity.ErrQueueFull) {
		t.Fatalf("Dispatch(job-3) error = %v, want ErrQueueFull", err)
	}
}

func TestDispatchAfterShutdownReturnsErrQueueClosed(t *testing.T) {
	pool, _, _ := newTestPool(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	if err := pool.Dispatch(context.Background(), "job-1"); !errors.Is(err, entity.ErrQueueClosed) {
		t.Fatalf("Dispatch() error = %v, want ErrQueueClosed", err)
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	pool, _, _ := newTestPool(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("first Shutdown() error = %v", err)
	}
	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown() error = %v — it must not panic or fail", err)
	}
}

func TestShutdownWaitsForInFlightJob(t *testing.T) {
	pool, repo, registry := newTestPool(t, nil)

	started := make(chan struct{})
	registry.Register("slow", func(context.Context, entity.Job) error {
		close(started)
		time.Sleep(150 * time.Millisecond)
		return nil
	})
	seedJob(t, repo, "job-1", "slow", 1)

	if err := pool.Dispatch(context.Background(), "job-1"); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	job, err := repo.FindByID(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if job.Status != entity.StatusCompleted {
		t.Fatalf("Status = %q after shutdown, want completed — shutdown abandoned in-flight work", job.Status)
	}
}
