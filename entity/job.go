package entity

import "time"

// Status is the lifecycle state of a job. These four values are exactly the four
// buckets exposed by the GraphQL JobStatus type, so every job is always counted in
// exactly one of them.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusFailed    Status = "failed"
	StatusCompleted Status = "completed"
)

// IsTerminal reports whether no further transition is possible from this state.
func (s Status) IsTerminal() bool {
	return s == StatusFailed || s == StatusCompleted
}

// Job is a unit of work. Attempts counts executions that have been started, so it is
// incremented at claim time, not at completion.
type Job struct {
	ID             string    `json:"id"`
	Task           string    `json:"task"`
	Status         Status    `json:"status"`
	Attempts       int32     `json:"attempts"`
	MaxAttempts    int32     `json:"max_attempts"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Clone returns an independent copy. Every field is a value type — time.Time's
// *Location pointer is immutable and shared safely — so a shallow copy is a deep copy.
// This is what allows the repository to hand jobs to concurrent readers without a race.
func (j Job) Clone() Job { return j }

// JobStatus is the aggregate returned by the GetAllJobStatus query.
type JobStatus struct {
	Pending   int32 `json:"pending"`
	Running   int32 `json:"running"`
	Failed    int32 `json:"failed"`
	Completed int32 `json:"completed"`
}

// Total is the sum of all four buckets. Because a job is always in exactly one state,
// this equals the number of jobs in the system.
func (s JobStatus) Total() int32 {
	return s.Pending + s.Running + s.Failed + s.Completed
}
