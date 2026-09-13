package logger

import (
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
)

func New(component string) *slog.Logger {
	isProd := os.Getenv("ENV") == "production"

	var handler slog.Handler
	if isProd {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
	} else {
		handler = tint.NewTextHandler(os.Stdout, &tint.Options{Level: slog.LevelDebug, TimeFormat: time.TimeOnly})
	}

	l := slog.New(handler)
	if component != "" {
		l = l.With("component", component)
	}
	return l
}
