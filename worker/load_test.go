package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"jobqueue/entity"
)

// TestPoolHandlesHundredConcurrentJobs enqueues 100 jobs from 100 goroutines and
// requires every one of them to reach a terminal state with the accounting intact.
func TestPoolHandlesHundredConcurrentJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("load test skipped in short mode")
	}

	pool, repo, registry := newTestPool(t, func(c *Config) {
		c.Workers = 8
		c.QueueSize = 256
	})

	registry.Register("fast", func(context.Context, entity.Job) error { return nil })
	registry.Register("flaky", UnstableHandler(1, 0))

	const total = 100
	var wg sync.WaitGroup
	wg.Add(total)
	for i := 0; i < total; i++ {
		go func(i int) {
			defer wg.Done()

			id := fmt.Sprintf("job-%03d", i)
			task := "fast"
			if i%5 == 0 { // one in five needs a retry
				task = "flaky"
			}
			seedJob(t, repo, id, task, 3)

			if err := pool.Dispatch(context.Background(), id); err != nil {
				t.Errorf("Dispatch(%s) error = %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		counts, err := repo.CountByStatus(context.Background())
		if err != nil {
			t.Fatalf("CountByStatus() error = %v", err)
		}
		if counts.Completed+counts.Failed == total {
			if counts.Total() != total {
				t.Fatalf("counts = %+v, total %d, want %d", counts, counts.Total(), total)
			}
			if counts.Failed != 0 {
				t.Fatalf("counts = %+v — every job should eventually succeed", counts)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	counts, _ := repo.CountByStatus(context.Background())
	t.Fatalf("jobs did not settle within 10s: %+v", counts)
}
