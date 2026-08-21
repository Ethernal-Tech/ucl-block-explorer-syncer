package syncer

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// capture builds a syncer whose logger writes JSON into the returned buffer.
func capture(t *testing.T) (*Syncer, *bytes.Buffer) {
	t.Helper()

	var buf bytes.Buffer

	return &Syncer{logger: slog.New(slog.NewJSONHandler(&buf, nil))}, &buf
}

// decode reads the single JSON record written to buf.
func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()

	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
		t.Fatalf("log line is not valid JSON (%v): %s", err, buf.String())
	}

	return record
}

// The whole point of the migration: what used to be one preformatted string is now a
// parseable record, so Loki can filter on component without regex over the message.
func TestLogEmitsStructuredJSON(t *testing.T) {
	s, buf := capture(t)

	s.log("indexed block %d", 42)

	record := decode(t, buf)

	if got := record["msg"]; got != "indexed block 42" {
		t.Errorf("msg = %v, want %q", got, "indexed block 42")
	}

	if got := record["component"]; got != "syncer" {
		t.Errorf("component = %v, want %q", got, "syncer")
	}

	if _, ok := record["time"]; !ok {
		t.Error("record has no time field; slog should supply one")
	}
}

// The hand-rolled "15:04:05.000 [syncer] " prefix has to be gone, or Loki ends up
// parsing the timestamp and component back out of the message forever.
func TestLogMessageCarriesNoEmbeddedPrefix(t *testing.T) {
	s, buf := capture(t)

	s.log("shut down")

	if got := decode(t, buf)["msg"]; got != "shut down" {
		t.Errorf("msg = %q, want %q with no timestamp or component prefix", got, "shut down")
	}
}

// logCtx is what makes a log line pivotable to its trace in Tempo.
func TestLogCtxInjectsTraceIDs(t *testing.T) {
	s, buf := capture(t)

	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "op")

	defer span.End()

	s.logCtx(ctx, "processing")

	record := decode(t, buf)
	sc := trace.SpanContextFromContext(ctx)

	if got := record["trace_id"]; got != sc.TraceID().String() {
		t.Errorf("trace_id = %v, want %v", got, sc.TraceID())
	}

	if got := record["span_id"]; got != sc.SpanID().String() {
		t.Errorf("span_id = %v, want %v", got, sc.SpanID())
	}
}

// Without an active span there is nothing to correlate to, and emitting empty
// trace_id fields would pollute every line the syncer writes outside a trace.
func TestLogCtxOmitsTraceIDsOutsideASpan(t *testing.T) {
	s, buf := capture(t)

	s.logCtx(context.Background(), "no span here")

	record := decode(t, buf)

	if _, ok := record["trace_id"]; ok {
		t.Error("trace_id present outside a span; expected it to be omitted")
	}
}

// A zero-value Syncer must not panic: tests construct these directly.
func TestLogOnZeroValueSyncerDoesNotPanic(t *testing.T) {
	s := &Syncer{}

	s.log("still fine")
}

// Failures must survive the default level. Logging them at info meant a syncer with a
// dead node produced no output at all and simply hung.
func TestErrorsAreVisibleAtWarnLevel(t *testing.T) {
	var buf bytes.Buffer

	s := &Syncer{logger: slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))}

	s.log("routine chatter")

	if buf.Len() != 0 {
		t.Errorf("info line leaked at warn level: %s", buf.String())
	}

	s.logErr("block worker encountered a fatal error: %s", "boom")

	record := decode(t, &buf)
	if got := record["level"]; got != "ERROR" {
		t.Errorf("level = %v, want ERROR", got)
	}

	if got := record["msg"]; got != "block worker encountered a fatal error: boom" {
		t.Errorf("msg = %v", got)
	}
}

// Retryable problems are warnings, not errors: they should be visible by default but
// must not read as terminal.
func TestWarningsAreDistinctFromErrors(t *testing.T) {
	var buf bytes.Buffer

	s := &Syncer{logger: slog.New(slog.NewJSONHandler(&buf, nil))}

	s.logWarn("RPC call failed: %v", "connection refused")

	if got := decode(t, &buf)["level"]; got != "WARN" {
		t.Errorf("level = %v, want WARN", got)
	}
}

// Error reporting must carry trace correlation too, or the one line an operator most
// wants to pivot from is the one line they cannot.
func TestLogErrCtxInjectsTraceIDs(t *testing.T) {
	s, buf := capture(t)

	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "op")

	defer span.End()

	s.logErrCtx(ctx, "cannot insert transactions: %v", "boom")

	record := decode(t, buf)
	if record["trace_id"] != trace.SpanContextFromContext(ctx).TraceID().String() {
		t.Errorf("trace_id = %v", record["trace_id"])
	}

	if record["level"] != "ERROR" {
		t.Errorf("level = %v, want ERROR", record["level"])
	}
}

// Level gating has to short-circuit before the fmt.Sprintf, since every one of the
// syncer's ~110 call sites formats its message eagerly.
func TestLogRespectsLevelGating(t *testing.T) {
	var buf bytes.Buffer

	s := &Syncer{logger: slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))}

	s.log("should not appear")

	if buf.Len() != 0 {
		t.Errorf("expected no output at error level, got %q", buf.String())
	}
}
