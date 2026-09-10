package constant

type contextKey string

const DataloaderContextKey contextKey = "dataloader"

// TaskUnstableJob is the task name reserved by the assignment for the handler that
// fails a fixed number of times before succeeding.
const TaskUnstableJob = "unstable-job"
