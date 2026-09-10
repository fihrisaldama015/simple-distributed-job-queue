package worker

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	const (
		base = 200 * time.Millisecond
		max  = 2 * time.Second
	)

	tests := []struct {
		name    string
		attempt int32
		want    time.Duration
	}{
		{"attempt 0 is clamped to the first delay", 0, 200 * time.Millisecond},
		{"after attempt 1", 1, 200 * time.Millisecond},
		{"after attempt 2 doubles", 2, 400 * time.Millisecond},
		{"after attempt 3 doubles again", 3, 800 * time.Millisecond},
		{"after attempt 4", 4, 1600 * time.Millisecond},
		{"growth is capped", 5, 2 * time.Second},
		{"large attempt stays capped", 40, 2 * time.Second},
		{"shift overflow stays capped", 70, 2 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Backoff(tt.attempt, base, max); got != tt.want {
				t.Fatalf("Backoff(%d) = %s, want %s", tt.attempt, got, tt.want)
			}
		})
	}
}
