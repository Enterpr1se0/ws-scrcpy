package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/app"
	"github.com/Genymobile/ws-scrcpy/go-backend/internal/applog"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, environ(), mustGetwd()); err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}

func run(ctx context.Context, env map[string]string, cwd string) error {
	applog.SetLevelFromEnv(env["WS_SCRCPY_LOG_LEVEL"])
	applog.Infof("starting ws-scrcpy-server cwd=%s log_level=%s", cwd, envOr(env, "WS_SCRCPY_LOG_LEVEL", "info"))
	return app.Run(ctx, app.Options{Env: env, CWD: cwd})
}

func envOr(env map[string]string, key string, fallback string) string {
	if value := env[key]; value != "" {
		return value
	}
	return fallback
}

func environ() map[string]string {
	env := make(map[string]string)
	for _, item := range os.Environ() {
		for i := 0; i < len(item); i++ {
			if item[i] == '=' {
				env[item[:i]] = item[i+1:]
				break
			}
		}
	}
	return env
}

func mustGetwd() string {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	return cwd
}
