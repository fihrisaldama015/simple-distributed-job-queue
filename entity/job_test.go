package entity

import "testing"

func TestStatusIsTerminal(t *testing.T) {
	tests := []struct {
		name   string
		status Status
		want   bool
	}{
		{"pending is not terminal", StatusPending, false},
		{"running is not terminal", StatusRunning, false},
		{"failed is terminal", StatusFailed, true},
		{"completed is terminal", StatusCompleted, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.IsTerminal(); got != tt.want {
				t.Fatalf("%s.IsTerminal() = %v, want %v", tt.status, got, tt.want)
			}
		})
	}
}

// TestCloneIsIndependent is the guarantee the whole concurrency story rests on:
// a job handed to a caller must never share memory with the stored job.
func TestCloneIsIndependent(t *testing.T) {
	original := Job{ID: "job-1", Task: "send-email", Status: StatusPending, Attempts: 1}

	clone := original.Clone()
	clone.Status = StatusCompleted
	clone.Attempts = 99
	clone.LastError = "boom"

	if original.Status != StatusPending || original.Attempts != 1 || original.LastError != "" {
		t.Fatalf("mutating the clone changed the original: %+v", original)
	}
}

func TestJobStatusTotal(t *testing.T) {
	counts := JobStatus{Pending: 1, Running: 2, Failed: 3, Completed: 4}
	if got := counts.Total(); got != 10 {
		t.Fatalf("Total() = %d, want 10", got)
	}
}
