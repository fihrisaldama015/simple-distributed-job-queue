package config

import (
	"jobqueue/pkg/server"
	"os"
	"strconv"
	"time"
)

var (
	// Data hold all configuration data
	Data config = getConfig()
)

type config struct {
	Server server.Config
	Queue  QueueConfig
}

// QueueConfig tunes the worker pool and the retry policy. Every field has a default
// that makes the assignment demo well; every field is overridable by an environment
// variable so tests can make execution instantaneous.
type QueueConfig struct {
	Workers          int           // goroutines consuming the queue
	QueueSize        int           // buffered channel capacity
	MaxAttempts      int32         // total attempts, including the first
	BaseBackoff      time.Duration // delay before attempt 2
	MaxBackoff       time.Duration // ceiling for the exponential growth
	TaskDuration     time.Duration // simulated work in the default task handler
	UnstableFailures int32         // failures injected by the unstable-job handler
	ShutdownGrace    time.Duration // how long shutdown waits for in-flight jobs
}

// DefaultQueueConfig returns the tuning defaults documented in the README.
func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		Workers:          8,
		QueueSize:        1024,
		MaxAttempts:      3,
		BaseBackoff:      200 * time.Millisecond,
		MaxBackoff:       2 * time.Second,
		TaskDuration:     1500 * time.Millisecond,
		UnstableFailures: 2,
		ShutdownGrace:    10 * time.Second,
	}
}

func getConfig() config {
	env := os.Getenv("MY_ENV")
	ConfigData := config{}
	switch env {
	case "staging":
		ConfigData.Server = server.Config{
			Port: 58577,
		}
	case "production":
		ConfigData.Server = server.Config{
			Port: 58578,
		}
	default:
		ConfigData.Server = server.Config{
			Port: 58579,
		}
	}
	ConfigData.Queue = queueConfigFromEnv(DefaultQueueConfig())
	return ConfigData
}

// queueConfigFromEnv applies environment overrides on top of base. A missing,
// unparseable or non-positive value leaves the corresponding default in place: a
// configuration typo must never produce a pool with zero workers that accepts jobs and
// never runs them.
func queueConfigFromEnv(base QueueConfig) QueueConfig {
	base.Workers = envInt("JOBQUEUE_WORKERS", base.Workers)
	base.QueueSize = envInt("JOBQUEUE_QUEUE_SIZE", base.QueueSize)
	base.MaxAttempts = int32(envInt("JOBQUEUE_MAX_ATTEMPTS", int(base.MaxAttempts)))
	base.UnstableFailures = int32(envInt("JOBQUEUE_UNSTABLE_FAILURES", int(base.UnstableFailures)))
	base.BaseBackoff = envDuration("JOBQUEUE_BASE_BACKOFF_MS", base.BaseBackoff)
	base.MaxBackoff = envDuration("JOBQUEUE_MAX_BACKOFF_MS", base.MaxBackoff)
	base.TaskDuration = envDuration("JOBQUEUE_TASK_DURATION_MS", base.TaskDuration)
	base.ShutdownGrace = envDuration("JOBQUEUE_SHUTDOWN_GRACE_MS", base.ShutdownGrace)
	return base
}

func envInt(name string, fallback int) int {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

// envDuration reads a whole number of milliseconds. Zero is accepted because tests
// legitimately set TaskDuration to 0 to make handlers return instantly.
func envDuration(name string, fallback time.Duration) time.Duration {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return fallback
	}
	return time.Duration(value) * time.Millisecond
}
