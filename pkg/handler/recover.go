package handler

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.uber.org/zap"
)

// Recover wraps Echo's panic-recovery middleware so a panic inside any HTTP handler
// (a GraphQL resolver, a dashboard handler) never takes the process down, and always
// leaves a structured log line behind - the "job.panic" event worker/pool.go already
// logs for a panicking task handler has an "http.panic" counterpart here, so an
// operator sees one consistent shape for "something panicked" regardless of which
// layer it happened in.
func Recover() echo.MiddlewareFunc {
	return middleware.RecoverWithConfig(middleware.RecoverConfig{
		LogErrorFunc: func(c echo.Context, err error, stack []byte) error {
			zap.L().Error("http.panic",
				zap.String("method", c.Request().Method),
				zap.String("path", c.Path()),
				zap.Error(err),
				zap.ByteString("stack", stack))
			return err
		},
	})
}
