package config

import (
	"testing"
	"time"
)

func TestDefaultQueueConfig(t *testing.T) {
	got := DefaultQueueConfig()

	if got.Workers != 8 {
		t.Errorf("Workers = %d, want 8", got.Workers)
	}
	if got.QueueSize != 1024 {
		t.Errorf("QueueSize = %d, want 1024", got.QueueSize)
	}
	if got.MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %d, want 3", got.MaxAttempts)
	}
	if got.BaseBackoff != 200*time.Millisecond {
		t.Errorf("BaseBackoff = %s, want 200ms", got.BaseBackoff)
	}
	if got.UnstableFailures != 2 {
		t.Errorf("UnstableFailures = %d, want 2", got.UnstableFailures)
	}
}

func TestQueueConfigFromEnvOverrides(t *testing.T) {
	t.Setenv("JOBQUEUE_WORKERS", "16")
	t.Setenv("JOBQUEUE_MAX_ATTEMPTS", "5")
	t.Setenv("JOBQUEUE_BASE_BACKOFF_MS", "50")

	got := queueConfigFromEnv(DefaultQueueConfig())

	if got.Workers != 16 {
		t.Errorf("Workers = %d, want 16", got.Workers)
	}
	if got.MaxAttempts != 5 {
		t.Errorf("MaxAttempts = %d, want 5", got.MaxAttempts)
	}
	if got.BaseBackoff != 50*time.Millisecond {
		t.Errorf("BaseBackoff = %s, want 50ms", got.BaseBackoff)
	}
	if got.QueueSize != 1024 {
		t.Errorf("QueueSize = %d, want the untouched default 1024", got.QueueSize)
	}
}

// An operator typo must not take the process down or silently produce a zero-worker
// pool that accepts jobs and never runs them.
func TestQueueConfigFromEnvIgnoresInvalidValues(t *testing.T) {
	t.Setenv("JOBQUEUE_WORKERS", "not-a-number")
	t.Setenv("JOBQUEUE_QUEUE_SIZE", "0")
	t.Setenv("JOBQUEUE_MAX_ATTEMPTS", "-3")

	got := queueConfigFromEnv(DefaultQueueConfig())

	if got.Workers != 8 {
		t.Errorf("Workers = %d, want the default 8", got.Workers)
	}
	if got.QueueSize != 1024 {
		t.Errorf("QueueSize = %d, want the default 1024", got.QueueSize)
	}
	if got.MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %d, want the default 3", got.MaxAttempts)
	}
}
