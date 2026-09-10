# Simple Distributed Job Queue Simulation
a simple job queue system built by golang

---

## Part 1 — Job Queue Backend (GraphQL)

### Requirement:
-> Go >= 1.20

### How to Run:
-> on VsCode -> Run(Pick Go-Debug)
-> open localhost:58579/graphiql
-> should be look like this
![Graphiql](./docs/graphiql.jpg)

### Requirements
make sure you complete all the function stated at first you load graphiql page
1. SimultaneousCreateJob -> build a simultaneous job
2. SimulateUnstableJob -> special case for task "unstable-job" fails twice before passing
3. GetAllJobs -> get all jobs that already registered
4. GetJobById -> get job by the id that been created by enqueue job
5. GetAllJobStatus -> get the stats of all jobs that been processed

### Evaluation Criteria:
* **Correctness**: Job creation, execution, status updates are accurate.
* **Concurrency Safety**: Multiple jobs created/processed at once → no race, no corruption.
* **Idempotency Handling**: Same job/task with same ID or token → doesn't process twice.
* **Retry Logic**: Failing job retries up to N times with delay.
* **In-memory Safety**: Maps/lists used safely under concurrent access.
* **Code Quality**: Idiomatic Go, good naming, clean package layout.
* **Clean Architecture**: Separation of domain, repository, resolver, GraphQL models.
* **Performance Awareness**: Handles 50–100 concurrent jobs without crash or slowdown.
* **Logging and Debugging**: Logs meaningful events.
* **Graceful Failure Handling**: No panics; job failure doesn't crash system.

---

## Part 2 — HTMX Dashboard

### Overview
Build a server-rendered dashboard at `/jobqueue/dashboard` using [HTMX](https://htmx.org/). The backend handler lives in `delivery/htmx/`; the HTML templates live in `web/htmx/`. No JavaScript framework is needed — HTMX attributes drive all partial page updates.

### Routes

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/jobqueue/dashboard` | Full HTML page shell |
| `GET` | `/jobqueue/dashboard/message` | Initial HTMX fragment entry point |

> Add more fragment endpoints under `/jobqueue/dashboard/...` as each section is built out.

---

### Backend (Go — `delivery/htmx/`)

Architecture rules:
- Handlers live in `delivery/htmx/` — the delivery layer only.
- Handlers that need data must call the service layer through the interfaces in `interface/`.
- No business logic inside handlers; handlers only bridge HTTP ↔ service.

#### Fragment Endpoints to Implement

| Endpoint | Description |
|----------|-------------|
| `POST /jobqueue/dashboard/jobs/create` | Runs `SimultaneousCreateJob` with Job1/Job2/Job3 from the form, returns updated job list fragment |
| `POST /jobqueue/dashboard/jobs/unstable` | Runs `SimulateUnstableJob`, returns updated job list fragment |
| `GET /jobqueue/dashboard/status` | Returns the status summary fragment (polled every 2 s) |
| `GET /jobqueue/dashboard/jobs` | Returns the full jobs table fragment (polled every 2 s) |
| `GET /jobqueue/dashboard/jobs/:id` | Returns the job detail fragment for a single job |

---

### Frontend (HTMX — `web/htmx/`)

#### 1. Action Bar
Three trigger buttons at the top of the page:

| Button | Fires | Notes |
|--------|-------|-------|
| **Create 3 Jobs** | `POST /jobqueue/dashboard/jobs/create` | Reads Job1/Job2/Job3 from the Variables Form |
| **Create Unstable Job** | `POST /jobqueue/dashboard/jobs/unstable` | Hard-coded `task: "unstable-job"` |
| **Refresh** *(optional)* | Re-polls `/jobqueue/dashboard/status` and `/jobqueue/dashboard/jobs` | Manual trigger |
| **Load Test (50 Jobs)** *(added)* | `POST /jobqueue/dashboard/jobs/loadtest` | Enqueues 50 jobs against the 8-worker pool - one in five is `unstable-job`, so some bounce back to `Pending` mid-drain to retry. `Create 3 Jobs` finishes inside one 2s poll tick, so pending/running never register there; this button makes both the concurrency ceiling and the retry policy visible together, watching `Pending` drain while `Running` sits pinned at 8. |

#### 2. Variables Form
Inline form whose values feed the **Create 3 Jobs** action. Defaults must match `web/variables.json`:

| Field | Default |
|-------|---------|
| Job1 | `JobTest1` |
| Job2 | `JobTest2` |
| Job3 | `JobTest3` |

#### 3. Status Summary
Live badge/card row — auto-polled every 2 seconds via `hx-trigger="every 2s"` targeting `#status-summary`:

- **Pending** / **Running** / **Failed** / **Completed**

#### 4. Jobs Table
Table auto-polled every 2 seconds targeting `#jobs-table`:

| Column | Field |
|--------|-------|
| ID | `job.id` |
| Task | `job.task` |
| Status | `job.status` |
| Attempts | `job.attempts` |
| View | Button → loads Job Detail Panel for that row's ID |

#### 5. Job Detail Panel
Panel or side section targeting `#job-detail`:
- Triggered by clicking **View** in any table row, or by typing a job ID into a search input.
- Displays: `id`, `task`, `status`, `attempts`.
- Swapped in via `hx-get="/jobqueue/dashboard/jobs/:id"` without reloading the rest of the page.

### How to Access
1. Start the server (see **How to Run** above).
2. Open `http://localhost:58579/jobqueue/dashboard`.
3. Status summary and job list auto-refresh every 2 seconds via HTMX polling.

# Good Luck Guys

---

# Implementation Notes

## Running

```bash
export PATH="$HOME/.local/go/bin:$PATH"   # only if Go is not already on your PATH
go run main.go
```

- GraphiQL: <http://localhost:58579/graphiql>
- Dashboard: <http://localhost:58579/jobqueue/dashboard>

VS Code users can still use the **Go-Debug** launch configuration. Stop the server with
Ctrl-C: it drains in-flight jobs before exiting.

## Tests

```bash
make test        # go test ./... -cover -race -count=1
make unittest    # short mode, skips the 100-job load test
```

The race detector is not optional here — it is the automated proof behind the
concurrency-safety requirements.

> Coverage note: `delivery/graphql/mutation`, `.../query` and `.../resolver` report
> 0.0% under plain `go test -cover`. They are exercised by
> `delivery/graphql/graphql_test.go`, and `-cover` only counts statements executed by
> tests *in the same package*. Measure them with
> `go test ./delivery/... -coverpkg=./delivery/...`.

## How it works

A fixed pool of worker goroutines consumes job ids from a bounded buffered channel.
`Enqueue` persists the job as `pending` and returns immediately — execution is
asynchronous, so the mutation answers with `status: "pending"` and `attempts: 0`, and
you poll `GetJobById` (or watch the dashboard) to see it progress.

A worker claims a job by compare-and-swapping its status from `pending` to `running`
inside the repository's write lock. Exactly one worker can win, which is what makes
execution idempotent: the same job id delivered twice runs once. A failing attempt
returns the job to `pending` and a timer re-dispatches it after an exponential backoff
(200 ms, then 400 ms) until `MaxAttempts` is reached, after which the job becomes
`failed` with the last error recorded. Handler panics are recovered and treated as
failed attempts, so a bad task can never take the process down.

The repository hands out copies rather than pointers into its map, so background
workers and HTTP readers never share a mutable struct.

### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `JOBQUEUE_WORKERS` | 8 | worker goroutines |
| `JOBQUEUE_QUEUE_SIZE` | 1024 | queue capacity before `Enqueue` reports backpressure |
| `JOBQUEUE_MAX_ATTEMPTS` | 3 | attempts before a job is marked failed |
| `JOBQUEUE_BASE_BACKOFF_MS` | 200 | delay before the second attempt |
| `JOBQUEUE_MAX_BACKOFF_MS` | 2000 | backoff ceiling |
| `JOBQUEUE_TASK_DURATION_MS` | 1500 | simulated work per attempt — long enough to watch a job sit `running` across a couple of 2s polls; set it to `150` (or lower) for a fast manual loop |
| `JOBQUEUE_UNSTABLE_FAILURES` | 2 | failures injected into `unstable-job` |
| `JOBQUEUE_SHUTDOWN_GRACE_MS` | 10000 | shutdown budget for in-flight jobs |

### Demonstrating the retry logic

`SimulateUnstableJob` returns `status: "pending", attempts: 0` because the queue is
asynchronous — the job has not run yet. Take the returned id and poll:

```graphql
query GetJobById { Job(id: "PASTE_ID") { id task status attempts } }
```

Within about five seconds it reads `status: "completed", attempts: 3` - each attempt,
failing or not, simulates the same work duration, so the retry cycle takes long enough
to actually watch rather than flashing past. The dashboard shows the same thing live:
click **Create Unstable Job** and watch the row climb `1/3 → 2/3 → 3/3`. While it's
retrying, the row also shows the reason the previous attempt failed - so a "running"
row you catch on attempt 2 or 3 reads as "retrying after a failure", not as a
duplicate of the first run. Open its detail panel to watch the same thing up close; it
polls itself every 2 seconds.

### Idempotency

`Enqueue` accepts an optional `idempotencyKey`. Two calls with the same key return the
same job and enqueue work once:

```graphql
mutation { Enqueue(task: "send-email", idempotencyKey: "order-42") { id status } }
```

The argument is optional, so every operation in `web/documentation.graphql` runs
unchanged.

