package metrics

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "ucl-block-explorer-syncer/rpc"

// InstrumentedRPCClient wraps *rpc.Client and records the duration of every
// CallContext / BatchCallContext invocation into [NodeRPCDuration], labelled by
// JSON-RPC method. It satisfies the rpcClient interface expected by workers and
// forwards all other client methods via embedding.
type InstrumentedRPCClient struct {
	*rpc.Client
}

func NewInstrumentedRPCClient(c *rpc.Client) *InstrumentedRPCClient {
	return &InstrumentedRPCClient{Client: c}
}

func (c *InstrumentedRPCClient) CallContext(
	ctx context.Context,
	result interface{},
	method string,
	args ...interface{},
) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, method, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now().UTC()
	err := c.Client.CallContext(ctx, result, method, args...)
	NodeRPCDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (c *InstrumentedRPCClient) BatchCallContext(ctx context.Context, b []rpc.BatchElem) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "batch", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now().UTC()
	err := c.Client.BatchCallContext(ctx, b)

	NodeRPCDuration.WithLabelValues("batch").Observe(time.Since(start).Seconds())

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}
