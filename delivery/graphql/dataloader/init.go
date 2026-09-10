package _dataloader

import (
	"context"

	_interface "jobqueue/interface"
	"jobqueue/pkg/constant"

	"github.com/graph-gophers/dataloader/v6"
	"github.com/labstack/echo/v4"
)

// GeneralDataloader ...
type GeneralDataloader struct {
	JobLoader *dataloader.Loader
	jobRepo   _interface.JobRepository
}

// EchoMiddelware installs a fresh dataloader in the request context.
//
// A DataLoader's cache is scoped to one request by design: a process-lifetime cache
// would keep serving the first version of every job it ever loaded, so a completed job
// would still report as pending. Building one per request gives correct reads and
// still batches every lookup within a single GraphQL operation.
func (g GeneralDataloader) EchoMiddelware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		perRequest := GeneralDataloader{jobRepo: g.jobRepo}
		perRequest.JobLoader = dataloader.NewBatchedLoader(
			perRequest.JobBatchFunc,
			dataloader.WithCache(dataloader.NewCache()),
		)

		oriReq := c.Request()
		ctx := context.WithValue(oriReq.Context(), constant.DataloaderContextKey, perRequest)
		c.SetRequest(oriReq.WithContext(ctx))
		return next(c)
	}
}

// FromContext returns the request-scoped dataloader. ok is false outside an HTTP
// request — in unit tests, for instance — and callers then fall back to the service.
func FromContext(ctx context.Context) (GeneralDataloader, bool) {
	loader, ok := ctx.Value(constant.DataloaderContextKey).(GeneralDataloader)
	return loader, ok
}

// Initiator ...
type Initiator func(r *GeneralDataloader) *GeneralDataloader

// New ...
func New() Initiator {
	return func(r *GeneralDataloader) *GeneralDataloader {
		return r
	}
}

// SetJobRepository ...
func (i Initiator) SetJobRepository(repo _interface.JobRepository) Initiator {
	return func(s *GeneralDataloader) *GeneralDataloader {
		i(s).jobRepo = repo
		return s
	}
}

// SetBatchFunction ...
func (i Initiator) SetBatchFunction() Initiator {
	return func(s *GeneralDataloader) *GeneralDataloader {
		i(s).JobLoader = dataloader.NewBatchedLoader(s.JobBatchFunc, dataloader.WithCache(dataloader.NewCache()))
		return s
	}
}

// Build ...
func (i Initiator) Build() *GeneralDataloader {
	return i(&GeneralDataloader{})
}
