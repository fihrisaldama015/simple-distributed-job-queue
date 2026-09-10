package mutation

import (
	"context"
	_dataloader "jobqueue/delivery/graphql/dataloader"
	"jobqueue/delivery/graphql/resolver"
	_interface "jobqueue/interface"
)

type JobMutation struct {
	jobService _interface.JobService
	dataloader *_dataloader.GeneralDataloader
}

// enqueueArgs mirrors the Enqueue arguments declared in the schema. Declaring it
// explicitly — rather than reusing entity.Job — keeps the resolver honest about which
// fields are actually inputs.
type enqueueArgs struct {
	Task           string
	IdempotencyKey *string
}

// Enqueue registers a job and returns it as enqueued: pending, with zero attempts.
// The job executes in the background; poll Job(id:) to observe its progress.
func (q JobMutation) Enqueue(ctx context.Context, args enqueueArgs) (*resolver.JobResolver, error) {
	idempotencyKey := ""
	if args.IdempotencyKey != nil {
		idempotencyKey = *args.IdempotencyKey
	}

	job, err := q.jobService.Enqueue(ctx, args.Task, idempotencyKey)
	if err != nil {
		return nil, err
	}

	return &resolver.JobResolver{
		Data:       *job,
		JobService: q.jobService,
		Dataloader: q.dataloader,
	}, nil
}

// NewJobMutation to create new instance
func NewJobMutation(jobService _interface.JobService, dataloader *_dataloader.GeneralDataloader) JobMutation {
	return JobMutation{
		jobService: jobService,
		dataloader: dataloader,
	}
}
