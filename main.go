package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"jobqueue/config"
	"jobqueue/delivery/graphql"
	_dataloader "jobqueue/delivery/graphql/dataloader"
	"jobqueue/delivery/graphql/mutation"
	"jobqueue/delivery/graphql/query"
	"jobqueue/delivery/graphql/schema"
	_htmx "jobqueue/delivery/htmx"
	"jobqueue/entity"
	"jobqueue/pkg/constant"
	"jobqueue/pkg/handler"
	"jobqueue/pkg/server"
	inmemrepo "jobqueue/repository/inmem"
	"jobqueue/service"
	"jobqueue/worker"
	"time"

	_graphql "github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"

	"github.com/labstack/echo"
	"github.com/labstack/echo/v4/middleware"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	setupLogger()
	e := server.New(config.Data.Server)
	e.Echo.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "${remote_ip} ${time_rfc3339_nano} \"${method} ${path}\" ${status} ${bytes_out} \"${referer}\" \"${user_agent}\"\n",
	}))
	e.Echo.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET, echo.POST, echo.OPTIONS},
	}))

	//graphql schema
	opts := make([]_graphql.SchemaOpt, 0)
	opts = append(opts, _graphql.SubscribeResolverTimeout(10*time.Second))

	//initialize in mem database
	inMemDb := make(map[string]*entity.Job)

	//set job repository
	jobRepository := inmemrepo.
		NewJobRepository().
		SetInMemConnection(inMemDb).
		Build()
	dataloader := _dataloader.
		New().
		SetJobRepository(jobRepository).
		SetBatchFunction().
		Build()

	//set the task handlers: anything unregistered falls back to simulated work
	taskRegistry := worker.NewRegistry(config.Data.Queue.TaskDuration)
	taskRegistry.Register(constant.TaskUnstableJob,
		worker.UnstableHandler(config.Data.Queue.UnstableFailures, config.Data.Queue.TaskDuration))

	//start the worker pool
	pool := worker.New(worker.Config{
		Workers:       config.Data.Queue.Workers,
		QueueSize:     config.Data.Queue.QueueSize,
		MaxAttempts:   config.Data.Queue.MaxAttempts,
		BaseBackoff:   config.Data.Queue.BaseBackoff,
		MaxBackoff:    config.Data.Queue.MaxBackoff,
		ShutdownGrace: config.Data.Queue.ShutdownGrace,
	}, jobRepository, taskRegistry, zap.L())
	pool.Start()

	//set job service
	jobService := service.NewJobService().
		SetJobRepository(jobRepository).
		SetJobDispatcher(pool).
		SetMaxAttempts(config.Data.Queue.MaxAttempts).
		SetLogger(zap.L()).
		Build()

	jobMutation := mutation.NewJobMutation(jobService, dataloader)
	jobQuery := query.NewJobQuery(jobService, dataloader)

	rootResolver := graphql.
		New().
		SetJobMutation(jobMutation).
		SetJobQuery(jobQuery).
		Build()

	graphqlSchema := _graphql.MustParseSchema(schema.String(), rootResolver, opts...)
	e.Echo.POST("/graphql",
		handler.GraphQLHandler(&relay.Handler{Schema: graphqlSchema}),
		dataloader.EchoMiddelware,
	)
	e.Echo.GET("/graphql",
		handler.GraphQLHandler(&relay.Handler{Schema: graphqlSchema}),
		dataloader.EchoMiddelware,
	)
	e.Echo.GET("/graphiql", handler.GraphiQLHandler)

	dashboardTemplates, err := _htmx.ParseTemplates("./web/htmx/*.html")
	if err != nil {
		zap.L().Fatal("dashboard.template_parse_failed", zap.Error(err))
	}
	dashboardDefaults, err := _htmx.LoadVariables("./web/variables.json")
	if err != nil {
		zap.L().Fatal("dashboard.variables_load_failed", zap.Error(err))
	}
	dashboard := _htmx.NewDashboardHandler(jobService, dashboardTemplates, dashboardDefaults, zap.L())

	e.Echo.GET("/jobqueue/dashboard", dashboard.Page)
	e.Echo.GET("/jobqueue/dashboard/message", dashboard.Message)
	e.Echo.POST("/jobqueue/dashboard/jobs/create", dashboard.CreateJobs)
	e.Echo.POST("/jobqueue/dashboard/jobs/unstable", dashboard.CreateUnstableJob)
	e.Echo.GET("/jobqueue/dashboard/status", dashboard.StatusSummary)
	e.Echo.GET("/jobqueue/dashboard/jobs", dashboard.JobsTable)
	e.Echo.GET("/jobqueue/dashboard/jobs/search", dashboard.JobSearch)
	e.Echo.GET("/jobqueue/dashboard/jobs/:id", dashboard.JobDetail)

	// Vendored so the dashboard works without internet access. Kept outside any
	// directory named "vendor" — .gitignore excludes those, which would silently drop
	// the file from the repository and break the dashboard on a fresh clone.
	e.Echo.File("/static/htmx.min.js", "./web/static/htmx.min.js")

	go func() {
		if err := e.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			zap.L().Fatal("server.start_failed", zap.Error(err))
		}
	}()

	// Stop accepting requests first, then let in-flight jobs finish.
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh

	zap.L().Info("server.stopping")
	ctx, cancel := context.WithTimeout(context.Background(), config.Data.Queue.ShutdownGrace)
	defer cancel()

	if err := e.Echo.Shutdown(ctx); err != nil {
		zap.L().Error("server.shutdown_failed", zap.Error(err))
	}
	if err := pool.Shutdown(ctx); err != nil {
		zap.L().Error("pool.shutdown_failed", zap.Error(err))
	}
	zap.L().Info("server.stopped")
}

func setupLogger() {
	configLogger := zap.NewDevelopmentConfig()
	configLogger.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	configLogger.DisableStacktrace = true
	logger, _ := configLogger.Build()
	zap.ReplaceGlobals(logger)
}
