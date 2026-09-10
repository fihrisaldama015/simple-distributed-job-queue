// Package htmx serves the server-rendered dashboard. It is a delivery layer only:
// handlers bind HTTP input, call the job service, and render a template. No business
// logic lives here.
package htmx

import (
	"errors"
	"html/template"
	"net/http"
	"strings"

	"jobqueue/entity"
	_interface "jobqueue/interface"
	"jobqueue/pkg/constant"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// loadTestJobCount is how many jobs the "Load Test" button fires at once. It sits
// inside the README's stated target ("50-100 concurrent jobs") and, against the
// default 8-worker pool, is large enough that most of them sit visibly pending while
// they wait their turn — the one place in the dashboard that shows the bounded worker
// pool actually bounding something.
const loadTestJobCount = 50

// loadTestTaskName is deliberately not registered with a specific handler: it falls
// through to the registry's default handler, the same simulated work every other
// plain job runs.
const loadTestTaskName = "load-test"

// loadTestUnstableRatio makes every fifth load-test job the unstable-job retry
// demo instead of a plain job, mirroring the ratio worker/load_test.go already
// proves survives under load. Without this, Load Test only shows the worker
// pool's concurrency ceiling; with it, it shows retries recovering while that
// ceiling is under pressure too.
const loadTestUnstableRatio = 5

// DashboardHandler serves the dashboard page and its HTMX fragments.
type DashboardHandler struct {
	jobService _interface.JobService
	tmpl       *template.Template
	defaults   Variables
	log        *zap.Logger
}

// ParseTemplates parses every dashboard template once, at startup. Parsing per request
// would re-read the files on each of the 2-second polls; failing here also turns a
// broken template into a startup error instead of a 500 on first click.
//
// html/template — not text/template — because task names come from user input.
func ParseTemplates(glob string) (*template.Template, error) {
	return template.ParseGlob(glob)
}

// NewDashboardHandler ...
func NewDashboardHandler(jobService _interface.JobService, tmpl *template.Template, defaults Variables, log *zap.Logger) *DashboardHandler {
	if log == nil {
		log = zap.NewNop()
	}
	return &DashboardHandler{jobService: jobService, tmpl: tmpl, defaults: defaults, log: log}
}

// Page serves the full HTML shell.
func (h *DashboardHandler) Page(c echo.Context) error {
	return h.render(c, "page", map[string]interface{}{"Defaults": h.defaults})
}

// Message serves the fragment the page loads on startup.
func (h *DashboardHandler) Message(c echo.Context) error {
	return h.render(c, "message", nil)
}

// CreateJobs runs the SimultaneousCreateJob scenario from the submitted form.
func (h *DashboardHandler) CreateJobs(c echo.Context) error {
	tasks := []string{
		h.formValueOr(c, "job1", h.defaults.Job1),
		h.formValueOr(c, "job2", h.defaults.Job2),
		h.formValueOr(c, "job3", h.defaults.Job3),
	}

	for _, task := range tasks {
		if _, err := h.jobService.Enqueue(c.Request().Context(), task, ""); err != nil {
			h.log.Error("dashboard.create_failed", zap.String("task", task), zap.Error(err))
			return h.renderError(c, "Could not create jobs: "+err.Error())
		}
	}

	return h.JobsTable(c)
}

// CreateUnstableJob enqueues the task reserved for the retry demonstration.
func (h *DashboardHandler) CreateUnstableJob(c echo.Context) error {
	if _, err := h.jobService.Enqueue(c.Request().Context(), constant.TaskUnstableJob, ""); err != nil {
		h.log.Error("dashboard.create_unstable_failed", zap.Error(err))
		return h.renderError(c, "Could not create the unstable job: "+err.Error())
	}
	return h.JobsTable(c)
}

// LoadTest fires loadTestJobCount jobs at once. It exists to make the bounded worker
// pool visible: with more jobs than workers, some sit pending until a slot frees up,
// which the 3-job "Create 3 Jobs" button finishes too fast to ever show.
func (h *DashboardHandler) LoadTest(c echo.Context) error {
	ctx := c.Request().Context()
	for i := 0; i < loadTestJobCount; i++ {
		task := loadTestTaskName
		if i%loadTestUnstableRatio == 0 {
			task = constant.TaskUnstableJob
		}
		if _, err := h.jobService.Enqueue(ctx, task, ""); err != nil {
			h.log.Error("dashboard.load_test_failed", zap.Int("enqueued", i), zap.Error(err))
			return h.renderError(c, "Could not create the load test jobs: "+err.Error())
		}
	}
	return h.JobsTable(c)
}

// StatusSummary renders the four status counters. Polled every 2 seconds.
func (h *DashboardHandler) StatusSummary(c echo.Context) error {
	counts, err := h.jobService.GetJobStatus(c.Request().Context())
	if err != nil {
		h.log.Error("dashboard.status_failed", zap.Error(err))
		return h.renderError(c, "Could not load the status summary.")
	}
	return h.render(c, "status", counts)
}

// JobsTable renders the job list. Polled every 2 seconds.
func (h *DashboardHandler) JobsTable(c echo.Context) error {
	// FormValue (not QueryParam) so this reads the filter bar whether it arrived on
	// a GET poll's query string or, via hx-include, on a POST action's form body -
	// Create/Unstable/Load Test all end by re-rendering this same table and should
	// respect whatever filter was active when the button was clicked.
	opts := _interface.JobListOptions{
		Status:      entity.Status(c.FormValue("status")),
		TaskQuery:   c.FormValue("task"),
		NewestFirst: c.FormValue("sort") == "newest",
	}

	jobs, err := h.jobService.ListJobs(c.Request().Context(), opts)
	if err != nil {
		h.log.Error("dashboard.jobs_failed", zap.Error(err))
		return h.renderError(c, "Could not load the job list.")
	}

	return h.render(c, "jobs_table", jobsTableView{
		Jobs:         jobs,
		FilterActive: opts.Status != "" || strings.TrimSpace(opts.TaskQuery) != "",
	})
}

// jobsTableView is what the "jobs_table" template renders. FilterActive lets the
// empty state tell "nothing matches your filter" apart from "the queue is genuinely
// empty" - two very different situations that look identical from an empty []*Job.
type jobsTableView struct {
	Jobs         []*entity.Job
	FilterActive bool
}

// JobDetail renders one job, addressed by path parameter.
func (h *DashboardHandler) JobDetail(c echo.Context) error {
	return h.renderJob(c, c.Param("id"))
}

// JobSearch renders one job, addressed by query parameter. HTMX cannot build a path
// segment from an input without JavaScript, so the search box posts the id this way.
func (h *DashboardHandler) JobSearch(c echo.Context) error {
	return h.renderJob(c, strings.TrimSpace(c.QueryParam("id")))
}

func (h *DashboardHandler) renderJob(c echo.Context, id string) error {
	if id == "" {
		return h.renderError(c, "Enter a job ID to look one up.")
	}

	job, err := h.jobService.GetJob(c.Request().Context(), id)
	if err != nil {
		if errors.Is(err, entity.ErrJobNotFound) {
			return h.renderError(c, "Job not found: "+id)
		}
		h.log.Error("dashboard.job_detail_failed", zap.String("job_id", id), zap.Error(err))
		return h.renderError(c, "Could not load that job.")
	}

	return h.render(c, "job_detail", job)
}

func (h *DashboardHandler) formValueOr(c echo.Context, field, fallback string) string {
	if value := strings.TrimSpace(c.FormValue(field)); value != "" {
		return value
	}
	return fallback
}

func (h *DashboardHandler) render(c echo.Context, name string, data interface{}) error {
	c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextHTMLCharsetUTF8)
	c.Response().WriteHeader(http.StatusOK)
	return h.tmpl.ExecuteTemplate(c.Response().Writer, name, data)
}

// renderError answers with HTTP 200 on purpose: HTMX does not swap non-2xx responses,
// so a 404 would leave the panel showing stale content with no explanation. The real
// cause is logged; the user sees a message where they are looking.
func (h *DashboardHandler) renderError(c echo.Context, message string) error {
	return h.render(c, "error", message)
}
