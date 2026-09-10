package graphql

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	_dataloader "jobqueue/delivery/graphql/dataloader"
	"jobqueue/delivery/graphql/mutation"
	"jobqueue/delivery/graphql/query"
	"jobqueue/delivery/graphql/schema"
	"jobqueue/entity"
	"jobqueue/pkg/constant"
	inmemrepo "jobqueue/repository/inmem"
	"jobqueue/service"
	"jobqueue/worker"

	_graphql "github.com/graph-gophers/graphql-go"
	"go.uber.org/zap"
)

func newTestSchema(t *testing.T) *_graphql.Schema {
	t.Helper()

	repo := inmemrepo.NewJobRepository().
		SetInMemConnection(make(map[string]*entity.Job)).
		Build()
	loaders := _dataloader.New().SetJobRepository(repo).SetBatchFunction().Build()

	registry := worker.NewRegistry(0)
	registry.Register(constant.TaskUnstableJob, worker.UnstableHandler(2, 0))

	pool := worker.New(worker.Config{
		Workers:       4,
		QueueSize:     128,
		MaxAttempts:   3,
		BaseBackoff:   time.Millisecond,
		MaxBackoff:    5 * time.Millisecond,
		ShutdownGrace: 5 * time.Second,
	}, repo, registry, zap.NewNop())
	pool.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pool.Shutdown(ctx)
	})

	jobService := service.NewJobService().
		SetJobRepository(repo).
		SetJobDispatcher(pool).
		SetMaxAttempts(3).
		Build()

	root := New().
		SetJobMutation(mutation.NewJobMutation(jobService, loaders)).
		SetJobQuery(query.NewJobQuery(jobService, loaders)).
		Build()

	return _graphql.MustParseSchema(schema.String(), root)
}

// exec runs an operation and fails the test on any GraphQL error.
func exec(t *testing.T, s *_graphql.Schema, operation, name string, vars map[string]interface{}, out interface{}) {
	t.Helper()

	response := s.Exec(context.Background(), operation, name, vars)
	if len(response.Errors) > 0 {
		t.Fatalf("operation %s returned errors: %v", name, response.Errors)
	}
	if out != nil {
		if err := json.Unmarshal(response.Data, out); err != nil {
			t.Fatalf("decoding %s response: %v (raw: %s)", name, err, response.Data)
		}
	}
}

// The operation text is copied verbatim from web/documentation.graphql.
const simultaneousCreateJob = `
mutation SimultaneousCreateJob($Job1: String!, $Job2: String!, $Job3: String!) {
  job1: Enqueue(task:$Job1) { id }
  job2: Enqueue(task:$Job2) { id }
  job3: Enqueue(task:$Job3) { id }
}`

const simulateUnstableJob = `
mutation SimulateUnstableJob {
  Enqueue(task: "unstable-job") { id attempts status }
}`

func TestSimultaneousCreateJob(t *testing.T) {
	s := newTestSchema(t)

	var created struct {
		Job1 struct{ ID string } `json:"job1"`
		Job2 struct{ ID string } `json:"job2"`
		Job3 struct{ ID string } `json:"job3"`
	}
	exec(t, s, simultaneousCreateJob, "SimultaneousCreateJob", map[string]interface{}{
		"Job1": "JobTest1", "Job2": "JobTest2", "Job3": "JobTest3",
	}, &created)

	ids := map[string]bool{created.Job1.ID: true, created.Job2.ID: true, created.Job3.ID: true}
	if len(ids) != 3 {
		t.Fatalf("expected 3 distinct ids, got %v", ids)
	}
	for name, id := range map[string]string{"job1": created.Job1.ID, "job2": created.Job2.ID, "job3": created.Job3.ID} {
		if id == "" {
			t.Fatalf("%s has an empty id", name)
		}
	}

	waitForCompleted(t, s, created.Job1.ID)
	waitForCompleted(t, s, created.Job2.ID)
	waitForCompleted(t, s, created.Job3.ID)
}

func TestSimulateUnstableJob(t *testing.T) {
	s := newTestSchema(t)

	var created struct {
		Enqueue struct {
			ID       string `json:"id"`
			Attempts int32  `json:"attempts"`
			Status   string `json:"status"`
		} `json:"Enqueue"`
	}
	exec(t, s, simulateUnstableJob, "SimulateUnstableJob", nil, &created)

	// The mutation answers with the job as enqueued — execution is asynchronous.
	if created.Enqueue.Status != string(entity.StatusPending) || created.Enqueue.Attempts != 0 {
		t.Fatalf("mutation returned %+v, want pending with 0 attempts", created.Enqueue)
	}

	job := waitForCompleted(t, s, created.Enqueue.ID)
	if job.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (two failures then a success)", job.Attempts)
	}
}

func TestGetAllJobs(t *testing.T) {
	s := newTestSchema(t)
	exec(t, s, simultaneousCreateJob, "SimultaneousCreateJob", map[string]interface{}{
		"Job1": "JobTest1", "Job2": "JobTest2", "Job3": "JobTest3",
	}, nil)

	var all struct {
		Jobs []struct {
			ID   string `json:"id"`
			Task string `json:"task"`
		} `json:"Jobs"`
	}
	exec(t, s, `query GetAllJobs { Jobs { id task } }`, "GetAllJobs", nil, &all)

	if len(all.Jobs) != 3 {
		t.Fatalf("Jobs returned %d entries, want 3", len(all.Jobs))
	}
	want := []string{"JobTest1", "JobTest2", "JobTest3"}
	for i, task := range want {
		if all.Jobs[i].Task != task {
			t.Fatalf("Jobs[%d].task = %q, want %q — ordering is not stable", i, all.Jobs[i].Task, task)
		}
	}
}

func TestGetJobByIdReturnsNullWhenMissing(t *testing.T) {
	s := newTestSchema(t)

	var out struct {
		Job *struct {
			ID string `json:"id"`
		} `json:"Job"`
	}
	exec(t, s, `query GetJobById { Job(id: "some-id") { id status attempts } }`, "GetJobById", nil, &out)

	if out.Job != nil {
		t.Fatalf("Job = %+v, want null for an unknown id", out.Job)
	}
}

func TestGetAllJobStatusTotalsMatchJobCount(t *testing.T) {
	s := newTestSchema(t)
	exec(t, s, simultaneousCreateJob, "SimultaneousCreateJob", map[string]interface{}{
		"Job1": "JobTest1", "Job2": "JobTest2", "Job3": "JobTest3",
	}, nil)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var out struct {
			JobStatus struct {
				Pending   int32 `json:"pending"`
				Running   int32 `json:"running"`
				Failed    int32 `json:"failed"`
				Completed int32 `json:"completed"`
			} `json:"JobStatus"`
		}
		exec(t, s, `query GetAllJobStatus { JobStatus { pending running failed completed } }`, "GetAllJobStatus", nil, &out)

		total := out.JobStatus.Pending + out.JobStatus.Running + out.JobStatus.Failed + out.JobStatus.Completed
		if total != 3 {
			t.Fatalf("status buckets total %d, want 3 — a job was lost or double-counted", total)
		}
		if out.JobStatus.Completed == 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("jobs did not all complete within 3s")
}

func TestEnqueueIsIdempotentPerKey(t *testing.T) {
	s := newTestSchema(t)

	const op = `mutation Create($key: String!) { Enqueue(task: "send-email", idempotencyKey: $key) { id } }`

	var first, second struct {
		Enqueue struct {
			ID string `json:"id"`
		} `json:"Enqueue"`
	}
	exec(t, s, op, "Create", map[string]interface{}{"key": "order-42"}, &first)
	exec(t, s, op, "Create", map[string]interface{}{"key": "order-42"}, &second)

	if first.Enqueue.ID != second.Enqueue.ID {
		t.Fatalf("ids differ (%q vs %q) — the idempotency key was ignored", first.Enqueue.ID, second.Enqueue.ID)
	}

	var all struct {
		Jobs []struct {
			ID string `json:"id"`
		} `json:"Jobs"`
	}
	exec(t, s, `query GetAllJobs { Jobs { id } }`, "GetAllJobs", nil, &all)
	if len(all.Jobs) != 1 {
		t.Fatalf("store holds %d jobs, want 1", len(all.Jobs))
	}
}

type jobView struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Attempts int32  `json:"attempts"`
}

func waitForCompleted(t *testing.T, s *_graphql.Schema, id string) jobView {
	t.Helper()

	const op = `query GetJobById($id: String!) { Job(id: $id) { id status attempts } }`
	deadline := time.Now().Add(3 * time.Second)
	var last jobView

	for time.Now().Before(deadline) {
		var out struct {
			Job *jobView `json:"Job"`
		}
		exec(t, s, op, "GetJobById", map[string]interface{}{"id": id}, &out)
		if out.Job != nil {
			last = *out.Job
			if last.Status == string(entity.StatusCompleted) {
				return last
			}
			if last.Status == string(entity.StatusFailed) {
				t.Fatalf("job %s failed: %+v", id, last)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("job %s did not complete within 3s (last seen: %+v)", id, last)
	return last
}

// The operations shipped in web/documentation.graphql are what a reviewer will run in
// GraphiQL, seeded with web/variables.json. Validating the real files against the real
// schema catches drift between them without executing anything.
//
// Use ValidateWithVariables, not Validate: graphql-go's Validate checks variable values
// too, so a parameterised operation with no variables supplied fails with
// `Variable "Job1" has invalid value null` even though the document is perfectly valid.
func TestShippedDocumentationValidatesAgainstTheSchema(t *testing.T) {
	s := newTestSchema(t)

	document, err := os.ReadFile("../../web/documentation.graphql")
	if err != nil {
		t.Fatalf("reading web/documentation.graphql: %v", err)
	}
	rawVariables, err := os.ReadFile("../../web/variables.json")
	if err != nil {
		t.Fatalf("reading web/variables.json: %v", err)
	}

	var variables map[string]interface{}
	if err := json.Unmarshal(rawVariables, &variables); err != nil {
		t.Fatalf("parsing web/variables.json: %v", err)
	}

	if queryErrors := s.ValidateWithVariables(string(document), variables); len(queryErrors) > 0 {
		t.Fatalf("web/documentation.graphql does not validate against the schema: %v", queryErrors)
	}
}
