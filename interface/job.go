package _interface

import (
	"context"
	"jobqueue/entity"
)

type JobService interface {
	Enqueue(ctx context.Context, taskName string) (string, error)
	GetAllJobs(ctx context.Context) (output entity.Job, err error)
}

type JobRepository interface {
	Save(ctx context.Context, job *entity.Job) error

	// SaveIfAbsent stores job under the idempotency key only if that key is unused.
	// created reports whether this call performed the insert; stored is the job now
	// associated with the key — the new one, or the pre-existing one.
	SaveIfAbsent(ctx context.Context, key string, job *entity.Job) (stored *entity.Job, created bool, err error)

	FindByID(ctx context.Context, id string) (*entity.Job, error)

	// FindByIDs returns only the ids that exist; missing ids are simply absent from
	// the map, which is not an error. Used by the GraphQL DataLoader to collapse N
	// lookups into one lock acquisition.
	FindByIDs(ctx context.Context, ids []string) (map[string]*entity.Job, error)

	FindAll(ctx context.Context) ([]*entity.Job, error)

	// Update applies mutate to the stored job atomically under the write lock and
	// returns a copy of the result. If mutate returns an error nothing is written and
	// that error is propagated — this is how compare-and-swap guards are built.
	//
	// mutate MUST NOT call back into the repository: the write lock is held.
	Update(ctx context.Context, id string, mutate func(*entity.Job) error) (*entity.Job, error)

	// CountByStatus aggregates every job into the four GraphQL status buckets.
	CountByStatus(ctx context.Context) (entity.JobStatus, error)
}

// TaskHandler executes one attempt of a job. It receives a copy of the job, so
// reading job.Attempts is race-free and mutating the copy is harmless. Returning a
// non-nil error marks the attempt as failed; a panic is recovered by the pool and
// treated identically.
type TaskHandler func(ctx context.Context, job entity.Job) error

// TaskRegistry resolves a task name to its handler. Implementations must never
// return nil: an unknown task name resolves to a default handler.
type TaskRegistry interface {
	Handler(task string) TaskHandler
}
