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
	FindByID(ctx context.Context, id string) (*entity.Job, error)

	// FindByIDs returns only the ids that exist; missing ids are simply absent from
	// the map, which is not an error. Used by the GraphQL DataLoader to collapse N
	// lookups into one lock acquisition.
	FindByIDs(ctx context.Context, ids []string) (map[string]*entity.Job, error)

	FindAll(ctx context.Context) ([]*entity.Job, error)
}
