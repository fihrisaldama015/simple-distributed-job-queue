package _dataloader

import (
	"context"
	"testing"
	"time"

	"jobqueue/entity"
	inmemrepo "jobqueue/repository/inmem"

	"github.com/graph-gophers/dataloader/v6"
)

func TestJobBatchFuncResolvesHitsAndMisses(t *testing.T) {
	ctx := context.Background()
	repo := inmemrepo.NewJobRepository().
		SetInMemConnection(make(map[string]*entity.Job)).
		Build()

	if err := repo.Save(ctx, &entity.Job{
		ID: "job-1", Task: "send-email", Status: entity.StatusPending, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	loader := New().SetJobRepository(repo).SetBatchFunction().Build()

	keys := dataloader.NewKeysFromStrings([]string{"job-1", "missing"})
	results := loader.JobBatchFunc(ctx, keys)

	if len(results) != 2 {
		t.Fatalf("batch returned %d results, want 2", len(results))
	}
	if results[0].Error != nil {
		t.Fatalf("hit carried an error: %v", results[0].Error)
	}
	job, ok := results[0].Data.(*entity.Job)
	if !ok || job.ID != "job-1" {
		t.Fatalf("hit resolved to %#v, want job-1", results[0].Data)
	}
	if results[1].Error != nil {
		t.Fatalf("miss carried an error: %v — a missing id is not an error", results[1].Error)
	}
	if results[1].Data != nil {
		t.Fatalf("miss resolved to %#v, want nil", results[1].Data)
	}
}

func TestFromContextReportsAbsence(t *testing.T) {
	if _, ok := FromContext(context.Background()); ok {
		t.Fatal("FromContext() reported a loader in a bare context")
	}
}
