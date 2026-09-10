package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// TestRecoverPreventsPanicFromCrashingTheProcess is the HTTP-layer counterpart to
// worker/pool_test.go's TestPoolRecoversHandlerPanic: a panicking handler must never
// take the process down, and it must leave a structured log line behind.
func TestRecoverPreventsPanicFromCrashingTheProcess(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	previous := zap.L()
	zap.ReplaceGlobals(zap.New(core))
	defer zap.ReplaceGlobals(previous)

	e := echo.New()
	e.Use(Recover())
	e.GET("/boom", func(c echo.Context) error {
		panic("handler exploded")
	})

	req := httptest.NewRequest(http.MethodGet, "/boom", nil)
	rec := httptest.NewRecorder()

	// Without Recover(), this call itself panics and fails the whole test binary
	// (proven separately) - reaching the assertions below is itself the proof.
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}

	entries := logs.FilterMessage("http.panic").All()
	if len(entries) != 1 {
		t.Fatalf("got %d http.panic log entries, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["path"] != "/boom" {
		t.Fatalf("logged path = %v, want /boom", fields["path"])
	}
	if fields["method"] != http.MethodGet {
		t.Fatalf("logged method = %v, want GET", fields["method"])
	}
}

// A route that never panics must be completely unaffected by the middleware.
func TestRecoverPassesThroughNormalRequests(t *testing.T) {
	e := echo.New()
	e.Use(Recover())
	e.GET("/ok", func(c echo.Context) error {
		return c.String(http.StatusOK, "fine")
	})

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || rec.Body.String() != "fine" {
		t.Fatalf("status = %d, body = %q, want 200/\"fine\"", rec.Code, rec.Body.String())
	}
}
