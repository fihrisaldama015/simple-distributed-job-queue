package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"jobqueue/entity"
	_interface "jobqueue/interface"
)

// Registry maps task names to handlers. Handlers are registered once at startup but
// read from every worker goroutine, so the map is guarded.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]_interface.TaskHandler
	fallback _interface.TaskHandler
}

// NewRegistry returns a registry whose default handler simulates defaultWork of work.
func NewRegistry(defaultWork time.Duration) *Registry {
	return &Registry{
		handlers: make(map[string]_interface.TaskHandler),
		fallback: SimulatedWork(defaultWork),
	}
}

// Register binds a handler to a task name, replacing any previous binding.
func (r *Registry) Register(task string, handler _interface.TaskHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[task] = handler
}

// Handler returns the handler for task, or the default handler when none is
// registered. It never returns nil.
func (r *Registry) Handler(task string) _interface.TaskHandler {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if handler, ok := r.handlers[task]; ok {
		return handler
	}
	return r.fallback
}

// SimulatedWork stands in for real work: it waits, and it respects cancellation so
// shutdown is not blocked by a long-running task.
func SimulatedWork(d time.Duration) _interface.TaskHandler {
	return func(ctx context.Context, job entity.Job) error {
		if d <= 0 {
			return ctx.Err()
		}

		timer := time.NewTimer(d)
		defer timer.Stop()

		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// UnstableHandler implements the assignment's "unstable-job": it fails the first
// `failures` attempts of a job and succeeds afterwards.
//
// The decision is derived from the job's own Attempts counter, which the pool has
// already incremented when claiming this run. The handler therefore holds no state:
// two unstable jobs running concurrently cannot interfere with each other.
//
// Every attempt simulates `work` first, whether it goes on to fail or succeed - a
// real failing call rarely errors instantly, and an instant failure would give the
// retry cycle no visible "running" dwell time for a 2s dashboard poll (or a human) to
// ever catch.
func UnstableHandler(failures int32, work time.Duration) _interface.TaskHandler {
	return func(ctx context.Context, job entity.Job) error {
		if err := SimulatedWork(work)(ctx, job); err != nil {
			return err
		}
		if job.Attempts <= failures {
			return fmt.Errorf("simulated failure on attempt %d of %d",
				job.Attempts, job.MaxAttempts)
		}
		return nil
	}
}
