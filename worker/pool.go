package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"jobqueue/entity"
	_interface "jobqueue/interface"

	"go.uber.org/zap"
)

// Config tunes the pool. Zero values are replaced with safe minimums in New.
type Config struct {
	Workers       int
	QueueSize     int
	MaxAttempts   int32
	BaseBackoff   time.Duration
	MaxBackoff    time.Duration
	ShutdownGrace time.Duration
}

// Pool runs jobs on a fixed set of goroutines fed by a bounded channel. The fixed
// size is the point: a goroutine per job would let a burst of enqueues spawn unbounded
// concurrency, which is exactly what this design avoids.
type Pool struct {
	cfg      Config
	repo     _interface.JobRepository
	registry _interface.TaskRegistry
	log      *zap.Logger

	queue    chan string
	quit     chan struct{}
	quitOnce sync.Once
	workers  sync.WaitGroup
	retries  sync.WaitGroup

	now func() time.Time
}

// New builds a pool. It does not start any goroutine — call Start.
func New(cfg Config, repo _interface.JobRepository, registry _interface.TaskRegistry, log *zap.Logger) *Pool {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	if log == nil {
		log = zap.NewNop()
	}

	return &Pool{
		cfg:      cfg,
		repo:     repo,
		registry: registry,
		log:      log,
		queue:    make(chan string, cfg.QueueSize),
		quit:     make(chan struct{}),
		now:      time.Now,
	}
}

// Start launches the worker goroutines.
func (p *Pool) Start() {
	p.workers.Add(p.cfg.Workers)
	for i := 0; i < p.cfg.Workers; i++ {
		go p.work(i)
	}
	p.log.Info("pool.started",
		zap.Int("workers", p.cfg.Workers),
		zap.Int("queue_size", p.cfg.QueueSize),
		zap.Int32("max_attempts", p.cfg.MaxAttempts))
}

// Dispatch enqueues a job id without ever blocking the caller. A blocking channel send
// here would stall an HTTP request behind unrelated background work; backpressure is
// surfaced as a typed error instead.
func (p *Pool) Dispatch(ctx context.Context, jobID string) error {
	select {
	case <-p.quit:
		return entity.ErrQueueClosed
	default:
	}

	select {
	case p.queue <- jobID:
		return nil
	default:
		return entity.ErrQueueFull
	}
}

// Shutdown stops the workers and waits for in-flight jobs, bounded by ctx. Jobs still
// waiting in the buffer stay pending: the store is in-memory and dies with the
// process, so draining the backlog would only slow shutdown down.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.quitOnce.Do(func() { close(p.quit) })

	done := make(chan struct{})
	go func() {
		p.workers.Wait()
		p.retries.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.log.Info("pool.shutdown", zap.Bool("drained", true))
		return nil
	case <-ctx.Done():
		p.log.Warn("pool.shutdown", zap.Bool("drained", false), zap.Error(ctx.Err()))
		return ctx.Err()
	}
}

func (p *Pool) work(workerID int) {
	defer p.workers.Done()

	for {
		select {
		case <-p.quit:
			return
		case jobID := <-p.queue:
			p.process(context.Background(), workerID, jobID)
		}
	}
}

// process runs one attempt of one job: claim it, execute it, settle it.
func (p *Pool) process(ctx context.Context, workerID int, jobID string) {
	job, err := p.claim(ctx, jobID)
	if err != nil {
		return // claim() has already logged at the right level
	}

	p.log.Debug("job.claimed",
		zap.String("job_id", job.ID), zap.String("task", job.Task),
		zap.Int32("attempt", job.Attempts), zap.Int("worker_id", workerID))

	start := p.now()
	runErr := p.safeRun(ctx, p.registry.Handler(job.Task), *job)
	elapsed := p.now().Sub(start)

	switch {
	case runErr == nil:
		// job.LastError already holds whatever the previous attempt failed with
		// (or "" if this succeeded on the first try) - keep it rather than wiping
		// it, so a job that failed twice before succeeding still says so instead
		// of looking identical to one that never failed at all.
		p.settle(ctx, job, entity.StatusCompleted, job.LastError)
		p.log.Info("job.completed",
			zap.String("job_id", job.ID), zap.String("task", job.Task),
			zap.Int32("attempt", job.Attempts), zap.Int64("duration_ms", elapsed.Milliseconds()))

	case job.Attempts >= job.MaxAttempts:
		p.settle(ctx, job, entity.StatusFailed, runErr.Error())
		p.log.Error("job.exhausted",
			zap.String("job_id", job.ID), zap.String("task", job.Task),
			zap.Int32("attempts", job.Attempts), zap.Error(runErr))

	default:
		p.log.Warn("job.attempt_failed",
			zap.String("job_id", job.ID), zap.Int32("attempt", job.Attempts),
			zap.Int32("max_attempts", job.MaxAttempts), zap.Error(runErr))
		p.scheduleRetry(ctx, job, runErr)
	}
}

// claim transitions pending -> running and increments Attempts, atomically. Because
// the compare-and-swap runs inside the repository's write lock, exactly one worker can
// win for a given job — this is the execution-level idempotency guarantee.
func (p *Pool) claim(ctx context.Context, jobID string) (*entity.Job, error) {
	job, err := p.repo.Update(ctx, jobID, func(j *entity.Job) error {
		if j.Status != entity.StatusPending {
			return entity.ErrNotClaimable
		}
		j.Status = entity.StatusRunning
		j.Attempts++
		j.UpdatedAt = p.now()
		return nil
	})

	switch {
	case errors.Is(err, entity.ErrNotClaimable):
		// Normal control flow: a duplicate delivery, or a job already finished.
		p.log.Debug("job.duplicate_delivery_skipped", zap.String("job_id", jobID))
		return nil, err
	case errors.Is(err, entity.ErrJobNotFound):
		p.log.Warn("job.missing_on_claim", zap.String("job_id", jobID))
		return nil, err
	case err != nil:
		p.log.Error("job.claim_failed", zap.String("job_id", jobID), zap.Error(err))
		return nil, err
	}

	return job, nil
}

// safeRun converts a panicking handler into a failed attempt. A task must never be
// able to kill the process.
func (p *Pool) safeRun(ctx context.Context, handler _interface.TaskHandler, job entity.Job) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			p.log.Error("job.panic",
				zap.String("job_id", job.ID),
				zap.Any("panic", recovered),
				zap.ByteString("stack", debug.Stack()))
			err = fmt.Errorf("%w: %v", entity.ErrTaskPanicked, recovered)
		}
	}()

	return handler(ctx, job)
}

// settle writes the job's next state. Every terminal and back-to-pending transition
// funnels through here so the write and its failure handling live in one place.
func (p *Pool) settle(ctx context.Context, job *entity.Job, status entity.Status, lastErr string) {
	if _, err := p.repo.Update(ctx, job.ID, func(j *entity.Job) error {
		j.Status = status
		j.LastError = lastErr
		j.UpdatedAt = p.now()
		return nil
	}); err != nil {
		p.log.Error("job.settle_failed",
			zap.String("job_id", job.ID),
			zap.String("target_status", string(status)),
			zap.Error(err))
	}
}

// scheduleRetry returns the job to pending and re-dispatches it after a backoff. The
// worker is not held during the wait: sleeping in the worker would let a handful of
// failing jobs starve the pool.
func (p *Pool) scheduleRetry(ctx context.Context, job *entity.Job, cause error) {
	delay := Backoff(job.Attempts, p.cfg.BaseBackoff, p.cfg.MaxBackoff)
	p.settle(ctx, job, entity.StatusPending, cause.Error())

	p.log.Warn("job.retry_scheduled",
		zap.String("job_id", job.ID),
		zap.Int32("attempt", job.Attempts),
		zap.Duration("delay", delay))

	jobID := job.ID
	p.retries.Add(1)
	time.AfterFunc(delay, func() {
		defer p.retries.Done()
		if err := p.Dispatch(context.Background(), jobID); err != nil {
			p.log.Error("job.redispatch_failed", zap.String("job_id", jobID), zap.Error(err))
		}
	})
}
