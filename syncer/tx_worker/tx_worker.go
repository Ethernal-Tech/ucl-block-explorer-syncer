package txworker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/tracing"
	"github.com/ethereum/go-ethereum/rpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// componentName labels every log line from this package, so Loki queries can
// filter to one worker type without parsing the message.
const componentName = "tx_worker"

// TxJob represents a unit of work assigned to a [TxWorker], defining the range of transactions
// within a block that the worker is responsible for fetching and processing.
type TxJob struct {
	// Ctx carries the trace context of the block that produced this job. Jobs cross a
	// channel into another goroutine, so this is the only way the worker's spans can be
	// parented to the block being indexed. Storing a context in a struct is normally
	// discouraged, but a queued work item is the documented exception: there is no call
	// stack to thread it through. Nil is tolerated and yields unparented spans.
	Ctx context.Context

	// Block is the block whose transactions are being processed.
	Block *types.Block

	// From is the index of the first tx in the block that the worker should process (inclusive).
	From uint64

	// To is the index of the last tx in the block that the worker should process (exclusive).
	To uint64
}

type rpcClient interface {
	BatchCallContext(ctx context.Context, b []rpc.BatchElem) error
}

// TxWorker is a long-lived worker that receives jobs through the [TxWorker.jobCh], processes the
// assigned job (fetching transactions and/or their receipts for a given range within a block),
// and signals completion by writing to [TxWorker.doneCh]. For details on work distribution, see
// [CreateJobs] within helper package.
type TxWorker struct {
	// client is the EVM-based RPC client used to fetch transactions and their receipts.
	client rpcClient

	// processTxsFn is a callback invoked once per job, with all transactions in the assigned
	// range fully fetched and populated. Returning an error from this function causes the worker
	// to stop and report the error via [TxWorker.errCh].
	processTxsFn func([]*types.Transaction) error

	// fetchTxDataFn is a callback invoked for each transaction hash to determine whether the
	// full transaction data should be fetched in addition to its receipt. When it returns false,
	// only the receipt is fetched.
	fetchTxDataFn func(hash string) bool

	// doneCh is the channel on which the worker sends a signal upon completing its currently
	// assigned job, or when it has finished shutting down gracefully due to [TxWorker.jobCh]
	// being closed.
	doneCh chan<- uint64

	// jobCh is the channel through which the worker receives jobs. Sending to this channel while
	// a previous job's completion has not been acknowledged via [TxWorker.doneCh] may result in
	// undefined behavior. Closing this channel signals the worker to stop processing and shut
	// down gracefully. Closing the channel can always be done safely without waiting for the
	// current job to complete, which is particularly useful when [TxWorker.maxRetries] is set
	// to a large value or indefinitely (-1) and [TxWorker.retryInterval] is high, as waiting
	// for the job to finish (successfully or with an error) may take a long time.
	jobCh <-chan TxJob

	// errCh is the channel on which the worker sends an error upon encountering a fatal failure.
	// A value sent to this channel indicates that the worker has already shut down gracefully.
	errCh chan<- struct {
		Err error
		Id  uint64
	}

	// Optional fields (settable through [NewTxWorker] constructor function):

	// logger records state changes and actions during the syncer's lifecycle. Defaults to
	// the process-wide slog logger; never silent.
	logger *slog.Logger

	// id is the unique identifier of the worker. By default, it is set to zero.
	id uint64

	// maxRetries is the maximum number of RPC request attempts for fetching a transaction and/or
	// its receipt before giving up. -1 denotes indefinitely. By default, the first failure is
	// treated as fatal.
	maxRetries int64

	// retryInterval specifies how long the worker waits between two consecutive RPC attempts.
	// By default, 2000 milliseconds.
	retryInterval uint64

	// batchSize is the number of RPC calls grouped into a single batch request. By default, 1,
	// meaning each RPC call is sent individually without batching.
	batchSize uint64
}

// NewTxWorker constructs a new [TxWorker] instance. None of the required parameters can be nil.
// processTxsFn is a callback invoked once for each job, see [TxWorker.processTxsFn] for details.
// fetchTxData is a callback that determines whether full transaction data should be fetched in
// addition to its receipt, see [TxWorker.fetchTxDataFn] for details.
//
// The following optional configurations are available (see their documentation for details):
//  1. WithLogger (default: the process-wide slog logger)
//  2. WithID (default: 0)
//  3. WithRetry (default: first failure is treated as fatal)
//  4. WithBatchSize (default, 1)
func NewTxWorker(
	client rpcClient,
	processTxsFn func([]*types.Transaction) error,
	fetchTxDataFn func(hash string) bool,
	doneCh chan<- uint64,
	jobCh <-chan TxJob,
	errCh chan<- struct {
		Err error
		Id  uint64
	},
	opts ...TxWorkerOption) (*TxWorker, error) {
	switch {
	case client == nil:
		return nil, fmt.Errorf("client cannot be nil")
	case processTxsFn == nil:
		return nil, fmt.Errorf("processTxs function cannot be nil")
	case fetchTxDataFn == nil:
		return nil, fmt.Errorf("fetchTxData function cannot be nil")
	case doneCh == nil:
		return nil, fmt.Errorf("done channel cannot be nil")
	case jobCh == nil:
		return nil, fmt.Errorf("job channel cannot be nil")
	case errCh == nil:
		return nil, fmt.Errorf("error channel cannot be nil")
	}

	worker := &TxWorker{
		// Never nil: components fall back to the process default so a syncer started
		// without WithLogger still reports what it is doing.
		logger: slog.Default(),

		client:        client,
		processTxsFn:  processTxsFn,
		fetchTxDataFn: fetchTxDataFn,
		doneCh:        doneCh,
		jobCh:         jobCh,
		errCh:         errCh,
		maxRetries:    100,
		retryInterval: 2000,
		batchSize:     1,
	}

	for _, o := range opts {
		if err := o(worker); err != nil {
			return nil, err
		}
	}

	return worker, nil
}

// Start starts the worker in a goroutine. It processes jobs received through [TxWorker.jobCh] by
// fetching transaction receipts and, if approved by [TxWorker.fetchTxDataFn], full transaction
// data in batches of [TxWorker.batchSize] RPC calls from an EVM node. Once all transactions in
// a job are fetched, it invokes [TxWorker.processTxsFn] and signals completion by sending to
// [TxWorker.doneCh]. When [TxWorker.jobCh] is closed, the worker shuts down gracefully. A value
// sent to [TxWorker.errCh] indicates that the worker has already shut down.
func (w *TxWorker) Start() error {
	if w.client == nil {
		return fmt.Errorf(
			"method must be invoked on an instance initialized through [NewTxWorker]")
	}

	batch := make([]rpc.BatchElem, 0, w.batchSize)

	go func() {
		w.log("started")

	main_loop:
		for job := range w.jobCh {
			txs := job.Block.Transactions[job.From:job.To]

			jobCtx := job.Ctx
			if jobCtx == nil {
				jobCtx = context.Background()
			}

			jobCtx, jobSpan := tracing.Tracer().Start(jobCtx, "tx.job",
				trace.WithAttributes(
					attribute.Int64("block.number", int64(job.Block.Number)),
					attribute.Int64("tx.range.from", int64(job.From)),
					attribute.Int64("tx.range.to", int64(job.To)),
				))

			w.logCtx(jobCtx, "job [%v-%v] received", job.From, job.To)

			for _, tx := range txs {
				// One span per transaction is what makes an individual transaction
				// findable by hash in the backend. The RPC calls below are batched
				// across transactions, so they are siblings of this span rather than
				// children of it - a batch belongs to the job, not to one transaction.
				_, txSpan := tracing.Tracer().Start(jobCtx, "tx.process",
					trace.WithAttributes(attribute.String("tx.hash", tx.Hash)))

				// Fetch full transaction data only if fetchTxDataFn approves.
				if w.fetchTxDataFn(tx.Hash) {
					if ok, err := w.fetchBatch(jobCtx, &batch, rpc.BatchElem{
						Method: "eth_getTransactionByHash",
						Args:   []any{tx.Hash},
						Result: tx,
					}); err != nil {
						txSpan.End()
						jobSpan.End()
						w.shutDown(err)

						return
					} else if !ok {
						txSpan.End()
						jobSpan.End()

						break main_loop
					}
				}

				// Transaction receipt is always fetched.
				if ok, err := w.fetchBatch(jobCtx, &batch, rpc.BatchElem{
					Method: "eth_getTransactionReceipt",
					Args:   []any{tx.Hash},
					Result: tx,
				}); err != nil {
					txSpan.End()
					jobSpan.End()
					w.shutDown(err)

					return
				} else if !ok {
					txSpan.End()
					jobSpan.End()

					break main_loop
				}

				txSpan.End()
			}

			// Flush any remaining batch elements that didn't reach batchSize.
			if len(batch) > 0 {
				if ok, err := w.sendBatch(jobCtx, batch); err != nil {
					jobSpan.End()
					w.shutDown(err)

					return
				} else if !ok {
					jobSpan.End()

					break main_loop
				}

				batch = batch[:0]
			}

			if err := w.processTxsFn(txs); err != nil {
				jobSpan.End()
				w.shutDown(fmt.Errorf("cannot process transactions: %w", err))

				return
			}

			w.doneCh <- w.id

			w.logCtx(jobCtx, "job processed")

			jobSpan.End()
		}

		w.shutDown(nil)

		w.doneCh <- w.id
	}()

	return nil
}

// shutDown gracefully shuts down the worker. If err is non-nil, it is sent to [TxWorker.errCh].
func (w *TxWorker) shutDown(err error) {
	if err != nil {
		w.logErr("%s", err.Error())

		w.errCh <- struct {
			Err error
			Id  uint64
		}{err, w.id}
	}

	w.log("shut down")
}

// log records a lifecycle event at info level. Prefer [TxWorker.logCtx] wherever a
// context is in scope: it adds the trace and span IDs that join the line to its trace.
func (w *TxWorker) log(msg string, args ...any) {
	w.emit(context.Background(), slog.LevelInfo, msg, args...)
}

// logCtx records a lifecycle event at info level, tagged with the active span's IDs.
func (w *TxWorker) logCtx(ctx context.Context, msg string, args ...any) {
	w.emit(ctx, slog.LevelInfo, msg, args...)
}

// logWarn reports a recoverable problem at warn level: something failed but the
// component is retrying or degrading rather than stopping.
func (w *TxWorker) logWarn(msg string, args ...any) {
	w.emit(context.Background(), slog.LevelWarn, msg, args...)
}

// logWarnCtx reports a recoverable problem at warn level, tagged with the span's IDs.
func (w *TxWorker) logWarnCtx(ctx context.Context, msg string, args ...any) {
	w.emit(ctx, slog.LevelWarn, msg, args...)
}

// logErr reports a failure at error level. Failures must not be logged through [TxWorker.log]:
// info is filtered out in normal operation, which would make the process fail silently.
func (w *TxWorker) logErr(msg string, args ...any) {
	w.emit(context.Background(), slog.LevelError, msg, args...)
}

// logErrCtx reports a failure at error level, tagged with the active span's IDs.
func (w *TxWorker) logErrCtx(ctx context.Context, msg string, args ...any) {
	w.emit(ctx, slog.LevelError, msg, args...)
}

// emit is the single place that formats a message and attaches the component identity
// and trace correlation fields. The level check short-circuits before the Sprintf, which
// matters because every call site formats eagerly.
func (w *TxWorker) emit(ctx context.Context, level slog.Level, msg string, args ...any) {
	// Tolerate a zero-value struct: tests construct these directly, and a missing
	// logger must not turn a log line into a nil dereference.
	logger := w.logger
	if logger == nil {
		logger = slog.Default()
	}

	if !logger.Enabled(ctx, level) {
		return
	}

	attrs := make([]any, 0, 8)
	attrs = append(attrs, "component", componentName, "worker_id", w.id)
	attrs = append(attrs, tracing.LogFields(ctx)...)

	logger.Log(ctx, level, fmt.Sprintf(msg, args...), attrs...)
}

// fetchBatch is responsible for fetching data through a batch RPC call. Fetching is deferred
// until a sufficient number of elements is reached (as defined by [TxWorker.batchSize]). In
// that case only the element is added to the batch. The first return value indicates whether
// the batch fetching was successful (see [TxWorker.sendBatch] for possible scenarios). Note
// that deferred fetching (currently not enough elements to fill the batch) is also considered
// a valid case, so (true, nil) is returned in that case as well.
func (w *TxWorker) fetchBatch(ctx context.Context, batch *[]rpc.BatchElem, elem rpc.BatchElem) (bool, error) {
	*batch = append(*batch, elem)

	if uint64(len(*batch)) < w.batchSize {
		return true, nil
	}

	ok, err := w.sendBatch(ctx, *batch)
	if !ok || err != nil {
		return ok, err
	}

	*batch = (*batch)[:0]

	return true, nil
}

// sendBatch executes the batch RPC call, retrying on failure up to [TxWorker.maxRetries] times.
// The first return value indicates whether the batch call was successful. Possible returns:
//  1. (true, nil)  - batch call was successful.
//  2. (false, nil) - batch call was not successful; [TxWorker.jobCh] was closed during a retry.
//  3. (false, err) - batch call was not successful; [TxWorker.maxRetries] was reached.
func (w *TxWorker) sendBatch(ctx context.Context, batch []rpc.BatchElem) (bool, error) {
	ctx, span := tracing.Tracer().Start(ctx, "rpc.batch",
		trace.WithAttributes(attribute.Int("rpc.batch.size", len(batch))))
	defer span.End()

	for i := int64(1); ; i++ {
		// context.TODO here used to leave every InstrumentedRPCClient span an orphaned
		// root; passing the job's context is what nests them under the block.
		if err := w.client.BatchCallContext(ctx, batch); err != nil {
			w.logWarnCtx(ctx, "(batch) RPC call failed: %v", err)

			// If [TxWorker.maxRetries] is -1, retry indefinitely.
			if i == w.maxRetries {
				w.logErrCtx(ctx, "giving up...")
				span.SetStatus(codes.Error, "batch RPC call failed")

				return false, fmt.Errorf("cannot execute (batch) RPC call: %w", err)
			}

			select {
			case <-w.jobCh:
				// jobCh guarantees that only closing is valid while a job is in progress
				// (sending would cause undefined behavior), so receiving here always means
				// the channel was closed and the worker should shut down gracefully.
				return false, nil
			case <-time.After(time.Duration(w.retryInterval) * time.Millisecond):
			}

			continue
		}

		break
	}

	// This should never happen! Log and proceed.
	for _, elem := range batch {
		if elem.Error != nil {
			w.logErr("%s RPC call failed for %v: %v", elem.Method, elem.Args[0], elem.Error)
		}
	}

	return true, nil
}
