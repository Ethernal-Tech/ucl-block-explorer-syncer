package metrics

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
)

// InstrumentedRPCClient wraps *rpc.Client and records the duration of every
// CallContext / BatchCallContext invocation into [NodeRPCDuration], labelled by
// JSON-RPC method. It satisfies the rpcClient interface expected by workers and
// forwards all other client methods via embedding.
type InstrumentedRPCClient struct {
	*rpc.Client
}

// NewInstrumentedRPCClient wraps c so its calls are timed.
func NewInstrumentedRPCClient(c *rpc.Client) *InstrumentedRPCClient {
	return &InstrumentedRPCClient{Client: c}
}

func (c *InstrumentedRPCClient) CallContext(
	ctx context.Context,
	result interface{},
	method string,
	args ...interface{},
) error {
	start := time.Now().UTC()
	err := c.Client.CallContext(ctx, result, method, args...)
	NodeRPCDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())

	return err
}

// BatchCallContext times a batch JSON-RPC call under the "batch" label.
func (c *InstrumentedRPCClient) BatchCallContext(ctx context.Context, b []rpc.BatchElem) error {
	start := time.Now().UTC()
	err := c.Client.BatchCallContext(ctx, b)

	NodeRPCDuration.WithLabelValues("batch").Observe(time.Since(start).Seconds())

	return err
}
