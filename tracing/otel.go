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

// Tracer is safe to call before Init; it returns a no-op until the global provider is set.
func Tracer() trace.Tracer {
	return otel.Tracer(instrumentationName)
}

// Init builds the trace provider (exporting over OTLP/gRPC to endpoint) and sets
// the global W3C trace-context propagator, which is what makes the syncer's
// outbound node RPC carry traceparent. endpoint may be a URL ("http://host:4317",
// http scheme = insecure) or a bare "host:4317" (treated as insecure). The
// returned shutdown function flushes and stops the provider on process exit.
func Init(ctx context.Context, endpoint string) (func(context.Context) error, error) {
	res, err := buildResource(ctx)
	if err != nil {
		return nil, err
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

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

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

func buildResource(ctx context.Context) (*resource.Resource, error) {
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = defaultServiceName
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
			semconv.DeploymentEnvironment(env),
		),
	)
}
