package service

import (
	"context"
	"strings"
	"time"

	"jobqueue/entity"
	_interface "jobqueue/interface"

	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
)

// maxTaskNameLength bounds a field that is stored, logged and rendered into HTML.
const maxTaskNameLength = 128

type jobService struct {
	jobRepo     _interface.JobRepository
	dispatcher  _interface.JobDispatcher
	maxAttempts int32
	log         *zap.Logger
	now         func() time.Time
}

// Initiator ...
type Initiator func(s *jobService) *jobService

// Enqueue validates the task, persists the job as pending and hands it to the
// dispatcher. It returns immediately: the job runs in the background.
func (q jobService) Enqueue(ctx context.Context, task string, idempotencyKey string) (*entity.Job, error) {
	task = strings.TrimSpace(task)
	if task == "" {
		return nil, entity.ErrInvalidTask
	}
	if len(task) > maxTaskNameLength {
		return nil, entity.ErrTaskNameTooLong
	}

	now := q.now()
	job := &entity.Job{
		ID:             uuid.NewV4().String(),
		Task:           task,
		Status:         entity.StatusPending,
		Attempts:       0,
		MaxAttempts:    q.maxAttempts,
		IdempotencyKey: idempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if idempotencyKey != "" {
		stored, created, err := q.jobRepo.SaveIfAbsent(ctx, idempotencyKey, job)
		if err != nil {
			return nil, err
		}
		if !created {
			q.log.Info("job.idempotent_hit",
				zap.String("job_id", stored.ID),
				zap.String("idempotency_key", idempotencyKey))
			return stored, nil
		}
		job = stored
	} else if err := q.jobRepo.Save(ctx, job); err != nil {
		return nil, err
	}

	if err := q.dispatcher.Dispatch(ctx, job.ID); err != nil {
		// Never leave a job stranded in pending with nobody to run it.
		if _, updateErr := q.jobRepo.Update(ctx, job.ID, func(j *entity.Job) error {
			j.Status = entity.StatusFailed
			j.LastError = err.Error()
			j.UpdatedAt = q.now()
			return nil
		}); updateErr != nil {
			q.log.Error("job.settle_failed", zap.String("job_id", job.ID), zap.Error(updateErr))
		}
		q.log.Error("job.dispatch_failed", zap.String("job_id", job.ID), zap.Error(err))
		return nil, err
	}

	q.log.Info("job.enqueued",
		zap.String("job_id", job.ID),
		zap.String("task", job.Task))
	return job, nil
}

// GetJob ...
func (q jobService) GetJob(ctx context.Context, id string) (*entity.Job, error) {
	return q.jobRepo.FindByID(ctx, id)
}

// GetAllJobs ...
func (q jobService) GetAllJobs(ctx context.Context) ([]*entity.Job, error) {
	jobs, err := q.jobRepo.FindAll(ctx)
	if err != nil {
		return nil, err
	}
	if jobs == nil {
		jobs = make([]*entity.Job, 0)
	}
	return jobs, nil
}

// GetJobStatus ...
func (q jobService) GetJobStatus(ctx context.Context) (entity.JobStatus, error) {
	return q.jobRepo.CountByStatus(ctx)
}

// NewJobService ...
func NewJobService() Initiator {
	return func(s *jobService) *jobService {
		return s
	}
}

// SetJobRepository ...
func (i Initiator) SetJobRepository(jobRepository _interface.JobRepository) Initiator {
	return func(s *jobService) *jobService {
		i(s).jobRepo = jobRepository
		return s
	}
}

// SetJobDispatcher ...
func (i Initiator) SetJobDispatcher(dispatcher _interface.JobDispatcher) Initiator {
	return func(s *jobService) *jobService {
		i(s).dispatcher = dispatcher
		return s
	}
}

// SetMaxAttempts ...
func (i Initiator) SetMaxAttempts(maxAttempts int32) Initiator {
	return func(s *jobService) *jobService {
		i(s).maxAttempts = maxAttempts
		return s
	}
}

// SetLogger ...
func (i Initiator) SetLogger(log *zap.Logger) Initiator {
	return func(s *jobService) *jobService {
		i(s).log = log
		return s
	}
}

// Build fills in safe defaults so a partially configured service cannot nil-panic.
func (i Initiator) Build() _interface.JobService {
	s := i(&jobService{})
	if s.maxAttempts <= 0 {
		s.maxAttempts = 1
	}
	if s.log == nil {
		s.log = zap.NewNop()
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s
}
