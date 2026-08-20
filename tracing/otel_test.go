package tracing

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// The SDK reads OTEL_TRACES_SAMPLER itself, and NewTracerProvider applies environment
// options before explicit ones — so passing sdktrace.WithSampler would silently override
// operator configuration. This test pins that behaviour: if someone "adds sampler support"
// by passing WithSampler in newTracerProvider, it fails.
func TestSamplerComesFromTheEnvironment(t *testing.T) {
	cases := []struct {
		name        string
		sampler     string
		arg         string
		wantSampled bool
	}{
		{name: "unset samples everything", wantSampled: true},
		{name: "always_on", sampler: "always_on", wantSampled: true},
		{name: "always_off drops everything", sampler: "always_off", wantSampled: false},
		{name: "zero ratio drops everything", sampler: "traceidratio", arg: "0", wantSampled: false},
		{name: "full ratio samples everything", sampler: "traceidratio", arg: "1", wantSampled: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_SAMPLER", tc.sampler)
			t.Setenv("OTEL_TRACES_SAMPLER_ARG", tc.arg)

			res, err := buildResource("test")
			if err != nil {
				t.Fatalf("buildResource: %v", err)
			}

			tp, err := newTracerProvider(context.Background(), res, "")
			if err != nil {
				t.Fatalf("newTracerProvider: %v", err)
			}

			t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

			_, span := tp.Tracer("test").Start(context.Background(), "probe")
			defer span.End()

			if got := span.SpanContext().IsSampled(); got != tc.wantSampled {
				t.Errorf("span sampled = %v, want %v", got, tc.wantSampled)
			}
		})
	}
}

func TestInstanceIDPrefersExplicitEnv(t *testing.T) {
	t.Setenv("OTEL_SERVICE_INSTANCE_ID", "syncer-7")

	if got := instanceID(); got != "syncer-7" {
		t.Errorf("instanceID() = %q, want %q", got, "syncer-7")
	}
}

func TestInstanceIDFallsBackToHostname(t *testing.T) {
	t.Setenv("OTEL_SERVICE_INSTANCE_ID", "")

	if got := instanceID(); got == "" {
		t.Error("instanceID() returned empty; expected a hostname or 'unknown'")
	}
}

func TestBuildResourceSetsServiceIdentity(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_SERVICE_NAMESPACE", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")

	res, err := buildResource("v1.2.3")
	if err != nil {
		t.Fatalf("buildResource: %v", err)
	}

	want := map[string]string{
		"service.name":      defaultServiceName,
		"service.namespace": defaultServiceNamespace,
		"service.version":   "v1.2.3",
	}

	got := map[string]string{}
	for _, attr := range res.Attributes() {
		got[string(attr.Key)] = attr.Value.AsString()
	}

	for key, wantVal := range want {
		if got[key] != wantVal {
			t.Errorf("%s = %q, want %q", key, got[key], wantVal)
		}
	}

	if got["service.instance.id"] == "" {
		t.Error("service.instance.id is unset; replicas would be indistinguishable in the backend")
	}
}

// Init must succeed with no collector configured, leaving spans valid but unexported,
// so dev and CI keep log/trace correlation without running a collector.
func TestInitWithoutEndpointStillProducesValidTraceIDs(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_TRACES_SAMPLER", "")

	ctx := context.Background()

	shutdown, err := Init(ctx, "", "test")
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
