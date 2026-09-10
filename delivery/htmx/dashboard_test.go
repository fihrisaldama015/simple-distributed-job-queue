package htmx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"jobqueue/entity"
	_interface "jobqueue/interface"
	inmemrepo "jobqueue/repository/inmem"
	"jobqueue/service"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

type recordingDispatcher struct{}

func (recordingDispatcher) Dispatch(context.Context, string) error { return nil }

func newTestHandler(t *testing.T) (*DashboardHandler, _interface.JobService) {
	t.Helper()

	tmpl, err := ParseTemplates("../../web/htmx/*.html")
	if err != nil {
		t.Fatalf("ParseTemplates() error = %v", err)
	}

	repo := inmemrepo.NewJobRepository().
		SetInMemConnection(make(map[string]*entity.Job)).
		Build()
	svc := service.NewJobService().
		SetJobRepository(repo).
		SetJobDispatcher(recordingDispatcher{}).
		SetMaxAttempts(3).
		Build()

	defaults := Variables{Job1: "JobTest1", Job2: "JobTest2", Job3: "JobTest3"}
	return NewDashboardHandler(svc, tmpl, defaults, zap.NewNop()), svc
}

func get(t *testing.T, path string) (echo.Context, *httptest.ResponseRecorder) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	return echo.New().NewContext(req, rec), rec
}

func postForm(t *testing.T, path string, values url.Values) (echo.Context, *httptest.ResponseRecorder) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(values.Encode()))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationForm)
	rec := httptest.NewRecorder()
	return echo.New().NewContext(req, rec), rec
}

func TestPageRendersFormDefaults(t *testing.T) {
	handler, _ := newTestHandler(t)
	c, rec := get(t, "/jobqueue/dashboard")

	if err := handler.Page(c); err != nil {
		t.Fatalf("Page() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	body := rec.Body.String()
	for _, want := range []string{"JobTest1", "JobTest2", "JobTest3", "hx-trigger", "every 2s", "job-detail"} {
		if !strings.Contains(body, want) {
			t.Errorf("page is missing %q", want)
		}
	}
}

func TestMessageFragment(t *testing.T) {
	handler, _ := newTestHandler(t)
	c, rec := get(t, "/jobqueue/dashboard/message")

	if err := handler.Message(c); err != nil {
		t.Fatalf("Message() error = %v", err)
	}
	if rec.Code != http.StatusOK || rec.Body.Len() == 0 {
		t.Fatalf("status = %d, body = %q", rec.Code, rec.Body.String())
	}
}

func TestCreateJobsEnqueuesTheSubmittedNames(t *testing.T) {
	handler, svc := newTestHandler(t)
	c, rec := postForm(t, "/jobqueue/dashboard/jobs/create", url.Values{
		"job1": {"alpha"}, "job2": {"beta"}, "job3": {"gamma"},
	})

	if err := handler.CreateJobs(c); err != nil {
		t.Fatalf("CreateJobs() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	jobs, err := svc.GetAllJobs(context.Background())
	if err != nil {
		t.Fatalf("GetAllJobs() error = %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("created %d jobs, want 3", len(jobs))
	}
	for i, want := range []string{"alpha", "beta", "gamma"} {
		if jobs[i].Task != want {
			t.Errorf("jobs[%d].Task = %q, want %q", i, jobs[i].Task, want)
		}
	}

	body := rec.Body.String()
	if !strings.Contains(body, "alpha") || !strings.Contains(body, "<table") {
		t.Fatalf("response is not the jobs table fragment: %q", body)
	}
}

func TestCreateJobsFallsBackToDefaultsForBlankFields(t *testing.T) {
	handler, svc := newTestHandler(t)
	c, _ := postForm(t, "/jobqueue/dashboard/jobs/create", url.Values{
		"job1": {""}, "job2": {"  "}, "job3": {"gamma"},
	})

	if err := handler.CreateJobs(c); err != nil {
		t.Fatalf("CreateJobs() error = %v", err)
	}

	jobs, err := svc.GetAllJobs(context.Background())
	if err != nil {
		t.Fatalf("GetAllJobs() error = %v", err)
	}
	tasks := []string{jobs[0].Task, jobs[1].Task, jobs[2].Task}
	want := []string{"JobTest1", "JobTest2", "gamma"}
	for i := range want {
		if tasks[i] != want[i] {
			t.Fatalf("tasks = %v, want %v", tasks, want)
		}
	}
}

func TestCreateUnstableJobUsesTheReservedTaskName(t *testing.T) {
	handler, svc := newTestHandler(t)
	c, _ := postForm(t, "/jobqueue/dashboard/jobs/unstable", url.Values{})

	if err := handler.CreateUnstableJob(c); err != nil {
		t.Fatalf("CreateUnstableJob() error = %v", err)
	}

	jobs, err := svc.GetAllJobs(context.Background())
	if err != nil {
		t.Fatalf("GetAllJobs() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].Task != "unstable-job" {
		t.Fatalf("jobs = %+v, want a single unstable-job", jobs)
	}
}

// The load test button is the only place in the dashboard that visually proves the
// "handles 50-100 concurrent jobs" criterion: 3 jobs finish faster than one 2s poll
// tick, so the summary card never shows anything but 0 -> 3. 50 jobs against an 8-slot
// pool spends real time queued as pending, which is the point.
func TestLoadTestEnqueuesFiftyJobs(t *testing.T) {
	handler, svc := newTestHandler(t)
	c, rec := postForm(t, "/jobqueue/dashboard/jobs/loadtest", url.Values{})

	if err := handler.LoadTest(c); err != nil {
		t.Fatalf("LoadTest() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	jobs, err := svc.GetAllJobs(context.Background())
	if err != nil {
		t.Fatalf("GetAllJobs() error = %v", err)
	}
	if len(jobs) != loadTestJobCount {
		t.Fatalf("created %d jobs, want %d", len(jobs), loadTestJobCount)
	}

	// One in five jobs is the unstable-job retry demo, mirroring the ratio
	// worker/load_test.go already uses to prove retries survive under load -
	// the dashboard button should demonstrate the same thing, not just the
	// worker pool's concurrency ceiling on its own.
	var plainCount, unstableCount int
	for i, job := range jobs {
		want := loadTestTaskName
		if i%5 == 0 {
			want = "unstable-job"
			unstableCount++
		} else {
			plainCount++
		}
		if job.Task != want {
			t.Fatalf("jobs[%d].Task = %q, want %q", i, job.Task, want)
		}
	}
	if unstableCount != 10 {
		t.Fatalf("unstableCount = %d, want 10 (one in five of %d)", unstableCount, loadTestJobCount)
	}
	if plainCount != 40 {
		t.Fatalf("plainCount = %d, want 40", plainCount)
	}

	if !strings.Contains(rec.Body.String(), "<table") {
		t.Fatalf("response is not the jobs table fragment: %q", rec.Body.String())
	}
}

func TestStatusSummaryRendersCounts(t *testing.T) {
	handler, svc := newTestHandler(t)
	if _, err := svc.Enqueue(context.Background(), "send-email", ""); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	c, rec := get(t, "/jobqueue/dashboard/status")
	if err := handler.StatusSummary(c); err != nil {
		t.Fatalf("StatusSummary() error = %v", err)
	}

	body := rec.Body.String()
	for _, want := range []string{"Pending", "Running", "Failed", "Completed"} {
		if !strings.Contains(body, want) {
			t.Errorf("status fragment is missing %q", want)
		}
	}
}

// A job sitting in "pending" (waiting to retry) or "running" (a second attempt) looks
// identical to a fresh, never-tried job unless its last failure is shown alongside the
// status - otherwise a retry reads as "just running", not "failed once, trying again".
func TestJobsTableShowsLastErrorForARetryingJob(t *testing.T) {
	handler, _ := newTestHandler(t)

	repo := inmemrepo.NewJobRepository().SetInMemConnection(make(map[string]*entity.Job)).Build()
	handler = NewDashboardHandler(
		service.NewJobService().SetJobRepository(repo).SetJobDispatcher(recordingDispatcher{}).SetMaxAttempts(3).Build(),
		handler.tmpl, handler.defaults, zap.NewNop(),
	)
	if err := repo.Save(context.Background(), &entity.Job{
		ID:          "job-1",
		Task:        "unstable-job",
		Status:      entity.StatusPending,
		Attempts:    1,
		MaxAttempts: 3,
		LastError:   "unstable-job: simulated failure on attempt 1 of 3",
	}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	c, rec := get(t, "/jobqueue/dashboard/jobs")
	if err := handler.JobsTable(c); err != nil {
		t.Fatalf("JobsTable() error = %v", err)
	}

	if !strings.Contains(rec.Body.String(), "simulated failure on attempt 1 of 3") {
		t.Fatalf("table does not show the retry reason: %q", rec.Body.String())
	}
}

func TestJobsTableEmptyState(t *testing.T) {
	handler, _ := newTestHandler(t)
	c, rec := get(t, "/jobqueue/dashboard/jobs")

	if err := handler.JobsTable(c); err != nil {
		t.Fatalf("JobsTable() error = %v", err)
	}
	if !strings.Contains(rec.Body.String(), "No jobs yet") {
		t.Fatalf("empty state missing: %q", rec.Body.String())
	}
}

func TestJobDetailByPathParam(t *testing.T) {
	handler, svc := newTestHandler(t)
	job, err := svc.Enqueue(context.Background(), "send-email", "")
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	c, rec := get(t, "/jobqueue/dashboard/jobs/"+job.ID)
	c.SetParamNames("id")
	c.SetParamValues(job.ID)

	if err := handler.JobDetail(c); err != nil {
		t.Fatalf("JobDetail() error = %v", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, job.ID) || !strings.Contains(body, "send-email") {
		t.Fatalf("detail fragment does not describe the job: %q", body)
	}
	if !strings.Contains(body, "every 2s") {
		t.Fatal("detail fragment should keep polling so retries are visible")
	}
	if !strings.Contains(body, "Created") || !strings.Contains(body, "Updated") {
		t.Fatalf("detail fragment is missing timestamps: %q", body)
	}
}

// A job that failed before eventually succeeding must still say so in its detail
// view - otherwise it looks identical to one that never failed at all.
func TestJobDetailShowsLastErrorEvenAfterSuccess(t *testing.T) {
	handler, _ := newTestHandler(t)

	repo := inmemrepo.NewJobRepository().SetInMemConnection(make(map[string]*entity.Job)).Build()
	handler = NewDashboardHandler(
		service.NewJobService().SetJobRepository(repo).SetJobDispatcher(recordingDispatcher{}).SetMaxAttempts(3).Build(),
		handler.tmpl, handler.defaults, zap.NewNop(),
	)
	if err := repo.Save(context.Background(), &entity.Job{
		ID:          "job-1",
		Task:        "unstable-job",
		Status:      entity.StatusCompleted,
		Attempts:    3,
		MaxAttempts: 3,
		LastError:   "unstable-job: simulated failure on attempt 2 of 3",
	}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	c, rec := get(t, "/jobqueue/dashboard/jobs/job-1")
	c.SetParamNames("id")
	c.SetParamValues("job-1")
	if err := handler.JobDetail(c); err != nil {
		t.Fatalf("JobDetail() error = %v", err)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "simulated failure on attempt 2 of 3") {
		t.Fatalf("completed job lost its failure history: %q", body)
	}
	if !strings.Contains(body, "before it succeeded") {
		t.Fatalf("completed job's error should be framed as history, not a live problem: %q", body)
	}
}

// HTMX ignores non-2xx responses, so an error must arrive as a 200 with a visible
// message rather than a status code nobody sees.
func TestJobDetailUnknownIDRendersErrorFragmentWith200(t *testing.T) {
	handler, _ := newTestHandler(t)
	c, rec := get(t, "/jobqueue/dashboard/jobs/nope")
	c.SetParamNames("id")
	c.SetParamValues("nope")

	if err := handler.JobDetail(c); err != nil {
		t.Fatalf("JobDetail() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 so HTMX swaps the fragment", rec.Code)
	}
	if !strings.Contains(strings.ToLower(rec.Body.String()), "not found") {
		t.Fatalf("body = %q, want a 'not found' message", rec.Body.String())
	}
}

func TestJobSearchByQueryParam(t *testing.T) {
	handler, svc := newTestHandler(t)
	job, err := svc.Enqueue(context.Background(), "send-email", "")
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	c, rec := get(t, "/jobqueue/dashboard/jobs/search?id="+job.ID)
	if err := handler.JobSearch(c); err != nil {
		t.Fatalf("JobSearch() error = %v", err)
	}
	if !strings.Contains(rec.Body.String(), job.ID) {
		t.Fatalf("search did not render the job: %q", rec.Body.String())
	}
}

func TestJobSearchWithoutIDRendersError(t *testing.T) {
	handler, _ := newTestHandler(t)
	c, rec := get(t, "/jobqueue/dashboard/jobs/search")

	if err := handler.JobSearch(c); err != nil {
		t.Fatalf("JobSearch() error = %v", err)
	}
	if rec.Code != http.StatusOK || rec.Body.Len() == 0 {
		t.Fatalf("status = %d, body = %q", rec.Code, rec.Body.String())
	}
}

// Task names come from user input and land in HTML: they must be escaped.
func TestTaskNamesAreHTMLEscaped(t *testing.T) {
	handler, svc := newTestHandler(t)
	if _, err := svc.Enqueue(context.Background(), `<script>alert(1)</script>`, ""); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	c, rec := get(t, "/jobqueue/dashboard/jobs")
	if err := handler.JobsTable(c); err != nil {
		t.Fatalf("JobsTable() error = %v", err)
	}
	if strings.Contains(rec.Body.String(), "<script>alert(1)</script>") {
		t.Fatal("task name was rendered unescaped — use html/template, not text/template")
	}
}
