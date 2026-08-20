// Package tracing wires up OpenTelemetry tracing for the syncer.
package tracing

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "ucl-block-explorer-syncer/tracing"

const defaultServiceName = "ucl-syncer"

// defaultServiceNamespace groups the syncer with the rest of the chain's services.
const defaultServiceNamespace = "ucl"

// Tracer is safe to call before Init; it returns a no-op until the global provider is set.
func Tracer() trace.Tracer {
	return otel.Tracer(instrumentationName)
}

// Init builds the trace provider and sets the global W3C trace-context propagator,
// which is what makes the syncer's outbound node RPC carry traceparent.
//
// endpoint may be a URL ("http://host:4317", http scheme = insecure) or a bare
// "host:4317" (treated as insecure). When empty, Init falls back to the standard
// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT / OTEL_EXPORTER_OTLP_ENDPOINT variables.
//
// With no endpoint configured from any source the provider is built without an
// exporter: spans still get valid trace IDs for log correlation and inbound
// traceparent is still honoured, but nothing is shipped. This keeps dev and CI
// free of a collector dependency without losing log/trace correlation.
//
// Configuration follows the standard OTel environment variables:
//   - OTEL_EXPORTER_OTLP_TRACES_ENDPOINT / OTEL_EXPORTER_OTLP_ENDPOINT
//   - OTEL_SERVICE_NAME (defaults to "ucl-syncer")
//   - OTEL_SERVICE_NAMESPACE (defaults to "ucl")
//   - OTEL_SERVICE_INSTANCE_ID (defaults to the hostname)
//   - OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG
//
// Sampling is deliberately left to the SDK, which reads OTEL_TRACES_SAMPLER itself
// and defaults to ParentBased(AlwaysSample). Do not pass sdktrace.WithSampler here:
// explicit options are applied after the environment ones, so doing so would
// silently override whatever operators configured.
//
// The returned shutdown function flushes and stops the provider on process exit.
func Init(ctx context.Context, endpoint, version string) (func(context.Context) error, error) {
	res, err := buildResource(version)
	if err != nil {
		return nil, err
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tp, err := newTracerProvider(ctx, res, endpoint)
	if err != nil {
		return nil, err
	}

	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

func buildResource(version string) (*resource.Resource, error) {
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = defaultServiceName
	}

	namespace := os.Getenv("OTEL_SERVICE_NAMESPACE")
	if namespace == "" {
		namespace = defaultServiceNamespace
	}

	env := os.Getenv("ENV")
	if env == "" {
		env = "dev"
	}

	// Schemaless avoids a Schema URL conflict with resource.Default(), whose
	// bundled semconv version differs from the one imported here.
	return resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName(serviceName),
			semconv.ServiceNamespace(namespace),
			semconv.ServiceInstanceID(instanceID()),
			semconv.ServiceVersion(version),
			semconv.DeploymentEnvironment(env),
		),
	)
}

// instanceID identifies this particular process. resource.Default only detects
// service.instance.id behind an experimental flag (OTEL_GO_X_RESOURCE), so it is
// set explicitly here: without it, spans from several syncer replicas are
// indistinguishable in the backend.
func instanceID() string {
	if id := os.Getenv("OTEL_SERVICE_INSTANCE_ID"); id != "" {
		return id
	}

	// In a container this is the pod/container name, which is what an operator
	// searches by.
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}

	return "unknown"
}

func newTracerProvider(
	ctx context.Context,
	res *resource.Resource,
	endpoint string,
) (*sdktrace.TracerProvider, error) {
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	}

	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}

	// No exporter: spans still get valid trace IDs and honour inbound traceparent.
	if endpoint == "" {
		return sdktrace.NewTracerProvider(sdktrace.WithResource(res)), nil
	}

	opts := []otlptracegrpc.Option{}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		opts = append(opts, otlptracegrpc.WithEndpointURL(endpoint))
	} else {
		opts = append(opts, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
	}

	exp, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	), nil
}
