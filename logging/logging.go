// Package logging configures structured logging (log/slog) for the syncer.
//
// JSON is the default output format because the syncer's logs are shipped to Loki,
// where a parseable record is what makes fields like trace_id queryable. Text output
// stays available for local development.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Init builds a logger at the given level ("debug"/"info"/"warn"/"error") and format
// ("json" or "text"), installs it as the slog default — which also routes the standard
// library log package through the same handler — and returns it.
//
// Components fall back to slog.Default() when no logger is supplied, so calling Init
// once at start-up configures the whole process. That fallback is deliberate: a syncer
// that logs nothing until explicitly configured is indistinguishable from a hung one.
func Init(level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: ParseLevel(level)}

	var handler slog.Handler
	if strings.EqualFold(strings.TrimSpace(format), "text") {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)

	return logger
}

// ParseLevel maps a level name to a slog.Level, falling back to info for anything
// unrecognised so a typo in configuration cannot silence the process.
func ParseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
