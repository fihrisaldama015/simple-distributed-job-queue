package query

import (
	"context"
	"errors"

	_dataloader "jobqueue/delivery/graphql/dataloader"
	"jobqueue/delivery/graphql/resolver"
	"jobqueue/entity"
	_interface "jobqueue/interface"
)

type JobQuery struct {
	jobService _interface.JobService
	dataloader *_dataloader.GeneralDataloader
}

// Jobs returns every job, oldest first.
func (q JobQuery) Jobs(ctx context.Context) ([]resolver.JobResolver, error) {
	jobs, err := q.jobService.GetAllJobs(ctx)
	if err != nil {
		return nil, err
	}

	resolvers := make([]resolver.JobResolver, 0, len(jobs))
	for _, job := range jobs {
		resolvers = append(resolvers, resolver.JobResolver{
			Data:       *job,
			JobService: q.jobService,
			Dataloader: q.dataloader,
		})
	}
	return resolvers, nil
}

// Job returns a single job, or null when the id is unknown. The schema declares
// `Job: Job` as nullable, so "not found" is an absent value rather than an error.
func (q JobQuery) Job(ctx context.Context, args struct {
	ID string
}) (*resolver.JobResolver, error) {
	job, err := q.jobService.GetJob(ctx, args.ID)
	if err != nil {
		if errors.Is(err, entity.ErrJobNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return &resolver.JobResolver{
		Data:       *job,
		JobService: q.jobService,
		Dataloader: q.dataloader,
	}, nil
}

// JobStatus returns the count of jobs in each state.
func (q JobQuery) JobStatus(ctx context.Context) (resolver.JobStatusResolver, error) {
	counts, err := q.jobService.GetJobStatus(ctx)
	if err != nil {
		return resolver.JobStatusResolver{}, err
	}

	return resolver.JobStatusResolver{
		Data:       counts,
		JobService: q.jobService,
		Dataloader: q.dataloader,
	}, nil
}

func NewJobQuery(jobService _interface.JobService,
	dataloader *_dataloader.GeneralDataloader) JobQuery {
	return JobQuery{
		jobService: jobService,
		dataloader: dataloader,
	}
}
