package entity

import "errors"

var (
	// ErrJobNotFound is returned by repository reads when no job carries that id.
	ErrJobNotFound = errors.New("job not found")

	// ErrInvalidTask rejects an empty or whitespace-only task name.
	ErrInvalidTask = errors.New("task name must not be empty")

	// ErrTaskNameTooLong rejects task names above the length limit.
	ErrTaskNameTooLong = errors.New("task name exceeds 128 characters")

	// ErrInvalidIdempotencyKey is returned when SaveIfAbsent is called with "".
	ErrInvalidIdempotencyKey = errors.New("idempotency key must not be empty")

	// ErrQueueFull signals backpressure: the dispatcher has no free slot.
	ErrQueueFull = errors.New("job queue is full")

	// ErrQueueClosed is returned by Dispatch after shutdown has begun.
	ErrQueueClosed = errors.New("job queue is closed")

	// ErrNotClaimable means the job was not pending when a worker tried to claim it.
	// This is normal control flow — it is how duplicate delivery is rejected — and is
	// logged at debug level, never as an error.
	ErrNotClaimable = errors.New("job is not in a claimable state")

	// ErrTaskPanicked wraps a value recovered from a panicking task handler.
	ErrTaskPanicked = errors.New("task handler panicked")
)
