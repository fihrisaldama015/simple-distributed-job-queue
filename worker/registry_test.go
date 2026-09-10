package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"jobqueue/entity"
)

func TestRegistryReturnsRegisteredHandler(t *testing.T) {
	registry := NewRegistry(0)
	called := false
	registry.Register("send-email", func(context.Context, entity.Job) error {
		called = true
		return nil
	})

	if err := registry.Handler("send-email")(context.Background(), entity.Job{}); err != nil {
		t.Fatalf("handler returned error = %v", err)
	}
	if !called {
		t.Fatal("registered handler was not invoked")
	}
}

// An unknown task must never produce a nil handler — that would panic a worker.
func TestRegistryFallsBackForUnknownTask(t *testing.T) {
	registry := NewRegistry(0)

	handler := registry.Handler("never-registered")
	if handler == nil {
		t.Fatal("Handler() returned nil for an unknown task")
	}
	if err := handler(context.Background(), entity.Job{Task: "never-registered"}); err != nil {
		t.Fatalf("default handler returned error = %v", err)
	}
}

func TestSimulatedWorkHonoursContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := SimulatedWork(5*time.Second)(ctx, entity.Job{})
	if err == nil {
		t.Fatal("SimulatedWork() returned nil for a cancelled context")
	}
}

func TestUnstableHandlerFailsTwiceThenSucceeds(t *testing.T) {
	handler := UnstableHandler(2, 0)

	tests := []struct {
		attempt int32
		wantErr bool
	}{
		{1, true},
		{2, true},
		{3, false},
		{4, false},
	}

	for _, tt := range tests {
		err := handler(context.Background(), entity.Job{ID: "job-1", Attempts: tt.attempt, MaxAttempts: 3})
		if tt.wantErr && err == nil {
			t.Fatalf("attempt %d: got nil, want an error", tt.attempt)
		}
		if !tt.wantErr && err != nil {
			t.Fatalf("attempt %d: got %v, want nil", tt.attempt, err)
		}
		if tt.wantErr && !strings.Contains(err.Error(), "unstable-job") {
			t.Fatalf("attempt %d: error %q should name the task", tt.attempt, err)
		}
	}
}
