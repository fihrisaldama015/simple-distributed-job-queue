package inmemrepo

import (
	"context"
	"jobqueue/entity"
	_interface "jobqueue/interface"
	"sort"
	"sync"
)

type jobRepository struct {
	mu      sync.RWMutex
	inMemDb map[string]*entity.Job
}

// Save stores a copy of job. Storing the caller's pointer would let the caller mutate
// the store afterwards, and would leave workers and HTTP readers sharing one struct.
func (t *jobRepository) Save(ctx context.Context, job *entity.Job) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	stored := job.Clone()
	t.inMemDb[stored.ID] = &stored
	return nil
}

// FindByID returns an independent copy of the job.
func (t *jobRepository) FindByID(ctx context.Context, id string) (*entity.Job, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	job, exists := t.inMemDb[id]
	if !exists {
		return nil, entity.ErrJobNotFound
	}
	out := job.Clone()
	return &out, nil
}

// FindByIDs resolves many ids under a single read lock.
func (t *jobRepository) FindByIDs(ctx context.Context, ids []string) (map[string]*entity.Job, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	found := make(map[string]*entity.Job, len(ids))
	for _, id := range ids {
		job, exists := t.inMemDb[id]
		if !exists {
			continue
		}
		out := job.Clone()
		found[id] = &out
	}
	return found, nil
}

// FindAll returns every job, oldest first. Map iteration order is random, so the
// result is sorted: without this the dashboard table reshuffles on every poll.
func (t *jobRepository) FindAll(ctx context.Context) ([]*entity.Job, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	jobs := make([]*entity.Job, 0, len(t.inMemDb))
	for _, job := range t.inMemDb {
		out := job.Clone()
		jobs = append(jobs, &out)
	}

	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			return jobs[i].ID < jobs[j].ID
		}
		return jobs[i].CreatedAt.Before(jobs[j].CreatedAt)
	})
	return jobs, nil
}

// Update is the only mutation path for an existing job. Read-modify-write happens
// entirely inside the write lock, so two workers can never interleave and lose an
// update, and a mutate error leaves the store untouched.
func (t *jobRepository) Update(ctx context.Context, id string, mutate func(*entity.Job) error) (*entity.Job, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	current, exists := t.inMemDb[id]
	if !exists {
		return nil, entity.ErrJobNotFound
	}

	draft := current.Clone()
	if err := mutate(&draft); err != nil {
		return nil, err
	}
	t.inMemDb[id] = &draft

	out := draft.Clone()
	return &out, nil
}

// Initiator ...
type Initiator func(s *jobRepository) *jobRepository

// NewJobRepository ...
func NewJobRepository() Initiator {
	return func(q *jobRepository) *jobRepository {
		return q
	}
}

// SetInMemConnection set database client connection
func (i Initiator) SetInMemConnection(inMemDb map[string]*entity.Job) Initiator {
	return func(s *jobRepository) *jobRepository {
		i(s).inMemDb = inMemDb
		return s
	}
}

// Build ...
func (i Initiator) Build() _interface.JobRepository {
	repo := i(&jobRepository{})
	if repo.inMemDb == nil {
		repo.inMemDb = make(map[string]*entity.Job)
	}
	return repo
}
