package txworker_test

import (
	"context"
	"testing"
	"time"

	txworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/tx_worker"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// installRecorder makes the global tracer provider record into an in-memory exporter
// for the duration of the test, since the worker resolves its tracer globally.
func installRecorder(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))

	previous := otel.GetTracerProvider()

	otel.SetTracerProvider(tp)

	t.Cleanup(func() {
		otel.SetTracerProvider(previous)

		_ = tp.Shutdown(context.Background())
	})

	return exporter
}

// runWorkerAndCollectSpans drives one job through a worker and returns the spans it produced.
func runWorkerAndCollectSpans(t *testing.T, job txworker.TxJob) tracetest.SpanStubs {
	t.Helper()

	return runWorkerAndCollectSpansWith(t, installRecorder(t), job)
}

// runWorkerAndCollectSpansWith is the variant used when the caller needs to start a parent
// span on the same provider before the worker runs.
func runWorkerAndCollectSpansWith(
	t *testing.T,
	exporter *tracetest.InMemoryExporter,
	job txworker.TxJob,
) tracetest.SpanStubs {
	t.Helper()

	jobCh := make(chan txworker.TxJob, 1)
	doneCh := make(chan uint64, 2)
	errCh := make(chan struct {
		Err error
		Id  uint64
	}, 1)

	client := new(mockRPCClient)
	client.On("BatchCallContext", mock.Anything, mock.Anything).Return(nil)

	worker, err := txworker.NewTxWorker(
		client,
		func(txs []*types.Transaction) error { return nil },
		func(hash string) bool { return true },
		doneCh,
		jobCh,
		errCh,
		txworker.WithID(1),
		txworker.WithBatchSize(1),
	)
	assert.NoError(t, err)
	assert.NoError(t, worker.Start())

	jobCh <- job

	select {
	case <-doneCh:
	case err := <-errCh:
		t.Fatalf("worker reported an error: %v", err.Err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the job to be processed")
	}

	close(jobCh)
	<-doneCh

	return exporter.GetSpans()
}

func testJob(ctx context.Context) txworker.TxJob {
	return txworker.TxJob{
		Ctx: ctx,
		Block: &types.Block{
			Number: 4242,
			Hash:   "0xblockhash",
			Transactions: []*types.Transaction{
				{Hash: "0xaaa1"},
				{Hash: "0xbbb2"},
			},
		},
		From: 0,
		To:   2,
	}
}

// The point of the whole exercise: an operator can find one transaction by its hash.
func TestPerTransactionSpansCarryTxHash(t *testing.T) {
	spans := runWorkerAndCollectSpans(t, testJob(context.Background()))

	found := map[string]bool{}

	for _, span := range spans {
		if span.Name != "tx.process" {
			continue
		}

		for _, attr := range span.Attributes {
			if attr.Key == "tx.hash" {
				found[attr.Value.AsString()] = true
			}
		}
	}

	assert.True(t, found["0xaaa1"], "no tx.process span carrying tx.hash 0xaaa1")
	assert.True(t, found["0xbbb2"], "no tx.process span carrying tx.hash 0xbbb2")
}

// The hash must live in an attribute, never in the span name: Tempo's metrics-generator
// emits a series per span name, so a hash there is an unbounded cardinality explosion.
func TestSpanNamesAreNotPerTransaction(t *testing.T) {
	spans := runWorkerAndCollectSpans(t, testJob(context.Background()))

	for _, span := range spans {
		assert.NotContains(t, span.Name, "0x",
			"span name %q embeds a hash; it belongs in an attribute", span.Name)
	}
}

// Worker spans must descend from the block's trace, which only works if TxJob carries
// the context across the channel into the worker goroutine.
func TestWorkerSpansJoinTheBlockTrace(t *testing.T) {
	exporter := installRecorder(t)

	blockCtx, blockSpan := otel.Tracer("test").Start(context.Background(), "block.index")
	traceID := blockSpan.SpanContext().TraceID()

	spans := runWorkerAndCollectSpansWith(t, exporter, testJob(blockCtx))

	blockSpan.End()

	assert.NotEmpty(t, spans, "worker produced no spans")

	for _, span := range spans {
		assert.Equal(t, traceID, span.SpanContext.TraceID(),
			"span %q is on a different trace than its block", span.Name)
	}
}

// A job with no trace context must still work: the syncer can dispatch before tracing
// is set up, and an unparented span is far better than a panic.
func TestNilJobContextDoesNotPanic(t *testing.T) {
	spans := runWorkerAndCollectSpans(t, txworker.TxJob{
		Block: &types.Block{
			Number:       1,
			Transactions: []*types.Transaction{{Hash: "0xccc3"}},
		},
		From: 0,
		To:   1,
	})

	assert.NotEmpty(t, spans)
}
