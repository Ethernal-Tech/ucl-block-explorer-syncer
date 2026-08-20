package tracing

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// LogFields returns trace_id/span_id from the active span as slog key-value pairs.
// Returns nil when ctx carries no valid span, so callers can pass it unconditionally:
//
//	logger.InfoContext(ctx, "block indexed", tracing.LogFields(ctx)...)
//
// These two fields are what let an operator pivot from a log line in Loki to the
// matching trace in Tempo, so they use the same names as the node's
// observability.LogFields.
func LogFields(ctx context.Context) []any {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}

	return []any{
		"trace_id", sc.TraceID().String(),
		"span_id", sc.SpanID().String(),
	}
}
