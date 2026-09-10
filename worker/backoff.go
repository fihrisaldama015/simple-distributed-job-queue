// Package worker executes queued jobs: it owns the dispatcher, the pool of worker
// goroutines, the retry policy and the task handler registry.
package worker

import "time"

// Backoff returns how long to wait before the attempt that follows `attempt`.
// The delay doubles each time — base, 2·base, 4·base … — and is capped at max.
//
// No jitter: there is no shared downstream to de-correlate here, and a deterministic
// delay keeps the retry tests exact instead of range-based.
func Backoff(attempt int32, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 62 { // shifting past this overflows int64
		return max
	}

	delay := base << (attempt - 1)
	if delay <= 0 || delay > max {
		return max
	}
	return delay
}
