package metrics

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
)

// InstrumentedRPCClient wraps a *rpc.Client and records the duration of every
// CallContext / BatchCallContext invocation into [NodeRPCDuration], labelled by
// JSON-RPC method.
//
// It embeds *rpc.Client, so it satisfies the (structural) rpcClient interfaces
// the block and tx workers expect and can be passed transparently in place of a
// raw client. Embedding also forwards Close and the rest of the client surface,
// so the original *rpc.Client can still own the connection lifecycle.
type InstrumentedRPCClient struct {
	*rpc.Client
}

// NewInstrumentedRPCClient wraps c so its calls are timed.
func NewInstrumentedRPCClient(c *rpc.Client) *InstrumentedRPCClient {
	return &InstrumentedRPCClient{Client: c}
}

// CallContext times a single JSON-RPC call and labels it by method.
func (c *InstrumentedRPCClient) CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error {
	start := time.Now()
	err := c.Client.CallContext(ctx, result, method, args...)
	NodeRPCDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
	return err
}

// BatchCallContext times a batch JSON-RPC call. The whole batch is recorded
// under the first element's method (batches are homogeneous in this codebase);
// an empty batch is labelled "batch".
func (c *InstrumentedRPCClient) BatchCallContext(ctx context.Context, b []rpc.BatchElem) error {
	start := time.Now()
	err := c.Client.BatchCallContext(ctx, b)
	method := "batch"
	if len(b) > 0 {
		method = b[0].Method
	}
	NodeRPCDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
	return err
}
