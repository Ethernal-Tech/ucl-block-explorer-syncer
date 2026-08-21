package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// unsetEnv clears a variable for the duration of the test. t.Setenv("X", "") sets it to
// the empty string, which is not the same thing: the SDK treats an empty
// OTEL_TRACES_SAMPLER as an unsupported value rather than as absent.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "") // registers cleanup to restore the original value
	_ = os.Unsetenv(key)
}

func attrsOf(res *resource.Resource) map[string]string {
	got := map[string]string{}
	for _, attr := range res.Attributes() {
		got[string(attr.Key)] = attr.Value.AsString()
	}

	return got
}

func newProviderForTest(t *testing.T) *sdktrace.TracerProvider {
	t.Helper()

	res, err := buildResourceFrom(resource.Empty(), "test")
	if err != nil {
		t.Fatalf("buildResourceFrom: %v", err)
	}

	tp, err := newTracerProvider(context.Background(), res, "")
	if err != nil {
		t.Fatalf("newTracerProvider: %v", err)
	}

	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	return tp
}

func TestBuildResourceSetsServiceIdentity(t *testing.T) {
	unsetEnv(t, "OTEL_SERVICE_NAME")
	unsetEnv(t, "OTEL_SERVICE_NAMESPACE")

	res, err := buildResourceFrom(resource.Empty(), "v1.2.3")
	if err != nil {
		t.Fatalf("buildResourceFrom: %v", err)
	}

	got := attrsOf(res)

	want := map[string]string{
		"service.name":      defaultServiceName,
		"service.namespace": defaultServiceNamespace,
		"service.version":   "v1.2.3",
	}

	for key, wantVal := range want {
		if got[key] != wantVal {
			t.Errorf("%s = %q, want %q", key, got[key], wantVal)
		}
	}

	if got["service.instance.id"] == "" {
		t.Error("service.instance.id is unset; replicas would be indistinguishable")
	}
}

// OTEL_RESOURCE_ATTRIBUTES is the standard way to set resource attributes and is what an
// operator reading the OpenTelemetry docs will reach for. Our defaults must not beat it.
func TestBuildResourceDefersToResourceAttributesEnv(t *testing.T) {
	unsetEnv(t, "OTEL_SERVICE_NAMESPACE")

	// Stands in for what resource.Default() produces when OTEL_RESOURCE_ATTRIBUTES is
	// set. Going through resource.Default() directly is unreliable: it memoises behind
	// a sync.Once, so whichever test runs first fixes the value for the process.
	base := resource.NewSchemaless(
		semconv.ServiceNamespace("from-env"),
		semconv.DeploymentEnvironment("from-env"),
		semconv.ServiceInstanceID("from-env"),
	)

	got := attrsOf(mustResource(t, base))

	for _, key := range []string{"service.namespace", "deployment.environment", "service.instance.id"} {
		if got[key] != "from-env" {
			t.Errorf("%s = %q, want %q (a hardcoded default overrode the environment)", key, got[key], "from-env")
		}
	}
}

func mustResource(t *testing.T, base *resource.Resource) *resource.Resource {
	t.Helper()

	res, err := buildResourceFrom(base, "v1.2.3")
	if err != nil {
		t.Fatalf("buildResourceFrom: %v", err)
	}

	return res
}

func TestInstanceIDPrefersExplicitEnv(t *testing.T) {
	t.Setenv("OTEL_SERVICE_INSTANCE_ID", "syncer-7")

	if got := instanceID(); got != "syncer-7" {
		t.Errorf("instanceID() = %q, want %q", got, "syncer-7")
	}
}

func TestInstanceIDFallsBackToHostname(t *testing.T) {
	unsetEnv(t, "OTEL_SERVICE_INSTANCE_ID")

	if got := instanceID(); got == "" {
		t.Error("instanceID() returned empty; expected a hostname or 'unknown'")
	}
}

// With no collector there is nothing to export to, so spans should not be recorded --
// otherwise every block builds a full span and throws it away. Trace IDs must survive
// regardless, or log correlation breaks.
func TestNoEndpointSkipsRecordingButKeepsTraceIDs(t *testing.T) {
	unsetEnv(t, "OTEL_EXPORTER_OTLP_ENDPOINT")
	unsetEnv(t, "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	unsetEnv(t, "OTEL_TRACES_SAMPLER")

	tp := newProviderForTest(t)

	ctx, span := tp.Tracer("test").Start(context.Background(), "probe")
	defer span.End()

	if span.IsRecording() {
		t.Error("span is recording with no exporter configured; the work is discarded")
	}

	if !span.SpanContext().IsValid() {
		t.Fatal("span context invalid; log correlation would break")
	}

	if fields := LogFields(ctx); len(fields) != 4 {
		t.Fatalf("LogFields = %v, want trace_id and span_id", fields)
	}
}

// An operator who sets the sampler explicitly must still get it, even with no exporter.
func TestExplicitSamplerWinsOverTheNoEndpointDefault(t *testing.T) {
	unsetEnv(t, "OTEL_EXPORTER_OTLP_ENDPOINT")
	unsetEnv(t, "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")

	tp := newProviderForTest(t)

	_, span := tp.Tracer("test").Start(context.Background(), "probe")
	defer span.End()

	if !span.IsRecording() {
		t.Error("OTEL_TRACES_SAMPLER=always_on was ignored")
	}
}

// The SDK reads OTEL_TRACES_SAMPLER itself and applies environment options before
// explicit ones, so passing WithSampler on the exporting path would override operators.
func TestSamplerComesFromTheEnvironment(t *testing.T) {
	unsetEnv(t, "OTEL_EXPORTER_OTLP_ENDPOINT")
	unsetEnv(t, "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")

	tp := newProviderForTest(t)

	_, span := tp.Tracer("test").Start(context.Background(), "probe")
	defer span.End()

	if span.SpanContext().IsSampled() {
		t.Error("OTEL_TRACES_SAMPLER=always_off was ignored")
	}
}

// The SDK reports export failures through a global handler that writes plain text to
// stderr. The syncer logs JSON, so those lines would corrupt the stream and an export
// outage would otherwise be invisible.
func TestSDKErrorsGoToTheLogger(t *testing.T) {
	unsetEnv(t, "OTEL_EXPORTER_OTLP_ENDPOINT")
	unsetEnv(t, "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")

	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	shutdown, err := Init(context.Background(), "", "test", logger)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}

	t.Cleanup(func() { _ = shutdown(context.Background()) })

	otel.Handle(errors.New("traces export: context deadline exceeded"))

	if buf.Len() == 0 {
		t.Fatal("SDK error did not reach the logger; it went to stderr as unstructured text")
	}

	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
		t.Fatalf("SDK error was not emitted as JSON (%v): %s", err, buf.String())
	}

	if record["level"] != "ERROR" {
		t.Errorf("level = %v, want ERROR", record["level"])
	}

	if !strings.Contains(record["err"].(string), "context deadline exceeded") {
		t.Errorf("err = %v, want the SDK's message", record["err"])
	}
}

func TestInitWithoutEndpointStillProducesValidTraceIDs(t *testing.T) {
	unsetEnv(t, "OTEL_EXPORTER_OTLP_ENDPOINT")
	unsetEnv(t, "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	unsetEnv(t, "OTEL_TRACES_SAMPLER")

	ctx := context.Background()

	shutdown, err := Init(ctx, "", "test", nil)
	if err != nil {
		t.Fatalf("Init with no endpoint: %v", err)
	}

	t.Cleanup(func() { _ = shutdown(context.Background()) })

	spanCtx, span := Tracer().Start(ctx, "unit")
	defer span.End()

	if !span.SpanContext().IsValid() {
		t.Fatal("span context is invalid; log correlation would be impossible")
	}

	if fields := LogFields(spanCtx); len(fields) != 4 {
		t.Fatalf("LogFields returned %v, want trace_id and span_id pairs", fields)
	}
}

func TestLogFieldsWithoutSpanReturnsNil(t *testing.T) {
	if fields := LogFields(context.Background()); fields != nil {
		t.Errorf("LogFields on a bare context = %v, want nil", fields)
	}
}

func TestLogFieldsCarriesTheActiveSpanIDs(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "op")

	defer span.End()

	fields := LogFields(ctx)
	if len(fields) != 4 {
		t.Fatalf("LogFields = %v, want 4 elements", fields)
	}

	sc := trace.SpanContextFromContext(ctx)
	if fields[1] != sc.TraceID().String() {
		t.Errorf("trace_id = %v, want %v", fields[1], sc.TraceID())
	}

	if fields[3] != sc.SpanID().String() {
		t.Errorf("span_id = %v, want %v", fields[3], sc.SpanID())
	}
}
