// Package tracing wires up OpenTelemetry tracing for the syncer.
package tracing

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
// Sampling is left to the SDK, which reads OTEL_TRACES_SAMPLER itself and defaults to
// ParentBased(AlwaysSample). Explicit options are applied after the environment ones, so
// passing sdktrace.WithSampler would override whatever operators configured. The single
// exception is the no-exporter path in newTracerProvider, which is documented there.
//
// The returned shutdown function flushes and stops the provider on process exit.
func Init(
	ctx context.Context,
	endpoint, version string,
	logger *slog.Logger,
) (func(context.Context) error, error) {
	if logger == nil {
		logger = slog.Default()
	}

	// The SDK reports its own failures - most commonly "traces export: context
	// deadline exceeded" when the collector is unreachable - through a global handler
	// that writes plain text to stderr via the standard log package. Since the syncer
	// logs JSON, those lines would arrive at the aggregator as unparseable noise, and
	// an export outage would otherwise be invisible.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Error("OpenTelemetry SDK error", "component", "tracing", "err", err.Error())
	}))

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

// buildResource describes this process to the backend.
func buildResource(version string) (*resource.Resource, error) {
	// resource.Default() memoises behind a sync.Once, so it reads the environment once
	// per process. That is correct here (this runs once at start-up) but means the
	// merge logic has to be tested through buildResourceFrom instead.
	return buildResourceFrom(resource.Default(), version)
}

// buildResourceFrom layers the syncer's defaults on top of base, skipping any attribute
// base already carries.
//
// resource.Default() includes the OTEL_RESOURCE_ATTRIBUTES detector and resource.Merge is
// last-value-wins, so passing our fallbacks unconditionally as the second argument would
// silently beat anything an operator set through the standard OTel variable.
func buildResourceFrom(base *resource.Resource, version string) (*resource.Resource, error) {
	present := make(map[attribute.Key]struct{}, len(base.Attributes()))
	for _, attr := range base.Attributes() {
		present[attr.Key] = struct{}{}
	}

	// resource.Default() always sets service.name (to "unknown_service:<binary>" when
	// nothing configured it), so presence alone cannot distinguish a real value from
	// that placeholder. OTEL_SERVICE_NAME is handled explicitly instead.
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = defaultServiceName
	}

	attrs := []attribute.KeyValue{semconv.ServiceName(serviceName)}

	if _, ok := present[semconv.ServiceNamespaceKey]; !ok {
		namespace := os.Getenv("OTEL_SERVICE_NAMESPACE")
		if namespace == "" {
			namespace = defaultServiceNamespace
		}

		attrs = append(attrs, semconv.ServiceNamespace(namespace))
	}

	if _, ok := present[semconv.ServiceInstanceIDKey]; !ok {
		attrs = append(attrs, semconv.ServiceInstanceID(instanceID()))
	}

	if _, ok := present[semconv.ServiceVersionKey]; !ok {
		attrs = append(attrs, semconv.ServiceVersion(version))
	}

	if _, ok := present[semconv.DeploymentEnvironmentKey]; !ok {
		env := os.Getenv("ENV")
		if env == "" {
			env = "dev"
		}

		attrs = append(attrs, semconv.DeploymentEnvironment(env))
	}

	// Schemaless avoids a Schema URL conflict with resource.Default(), whose
	// bundled semconv version differs from the one imported here.
	return resource.Merge(base, resource.NewSchemaless(attrs...))
}

// instanceID identifies this particular process. resource.Default only detects
// service.instance.id behind an experimental flag (OTEL_GO_X_RESOURCE), so it is
// set explicitly: without it, spans from several syncer replicas are
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

	// No exporter configured. Spans must still carry valid trace IDs so logs stay
	// correlatable, but there is nothing to export them to, and the default sampler
	// would have every block and transaction build a full span only to discard it. A
	// non-recording span still carries a generated trace ID, which is all LogFields needs.
	//
	// This is the one place an explicit sampler is justified, and it defers to
	// OTEL_TRACES_SAMPLER when an operator has set it: elsewhere, passing WithSampler
	// would override the environment, since NewTracerProvider applies env options
	// before explicit ones.
	if endpoint == "" {
		opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
		if _, explicit := os.LookupEnv("OTEL_TRACES_SAMPLER"); !explicit {
			opts = append(opts, sdktrace.WithSampler(sdktrace.NeverSample()))
		}

		return sdktrace.NewTracerProvider(opts...), nil
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
