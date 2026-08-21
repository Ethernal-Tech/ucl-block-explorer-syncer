package blockworker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/tracing"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// componentName labels every log line from this package, so Loki queries can
// filter to one worker type without parsing the message.
const componentName = "block_worker"

type rpcClient interface {
	CallContext(
		ctx context.Context,
		result interface{},
		method string,
		args ...interface{}) error
}

// BlockWorker is a long-lived worker that sequentially fetches and processes blocks from an
// EVM-based node, starting from [BlockWorker.startBlock]. It signals completion by writing
// to [BlockWorker.doneCh] - potentially never, if [BlockWorker.lastBlock] is not set. It can
// be paused and resumed via [BlockWorker.ctrlCh].
type BlockWorker struct {
	// client is the EVM-based RPC client used to fetch blocks.
	client rpcClient

	// processBlockFn is a callback invoked for each fetched block. Returning an error from this
	// function causes the worker to stop and report the error via [BlockWorker.errCh].
	processBlockFn func(block *types.Block) error

	// ctrlCh is the channel through which an external caller can pause and resume the worker.
	// Sending a signal pauses the worker, sending another resumes it, alternating with each
	// signal. When the channel is closed (and all previous signals are processed), the worker
	// shuts down gracefully and sends a signal to [BlockWorker.doneCh].
	ctrlCh <-chan struct{}

	// doneCh is the channel on which the worker sends a signal upon completing its work (by
	// reaching [BlockWorker.lastBlock]), or when it has finished shutting down gracefully due
	// to [BlockWorker.ctrlCh] being closed.
	doneCh chan<- struct{}

	// errCh is the channel on which the worker sends an error upon encountering a fatal failure.
	// A value sent to this channel indicates that the worker has already shut down gracefully.
	errCh chan<- error

	// Optional fields (settable through [NewBlockWorker] constructor function):

	// logger records state changes and actions during the syncer's lifecycle. Defaults to
	// the process-wide slog logger; never silent.
	logger *slog.Logger

	// id is the unique identifier of the worker. By default, it is set to zero.
	id uint64

	// startBlock is the block number from which the worker begins processing. By default, 0.
	startBlock uint64

	// lastBlock is the last block the worker will process, inclusive. Once reached, the worker
	// shuts down gracefully. By default, not set, meaning the worker runs indefinitely.
	//
	// Note: it is a pointer so that a value of zero (0) can be distinguished from the field not
	// being set at all.
	lastBlock *uint64

	// pollInterval specifies how often the worker attempts to fetch and process new blocks. By
	// default, 2000 milliseconds.
	pollInterval uint64

	// tipOnly controls when the pollInterval is applied. When true, the worker runs without any
	// delay until it reaches the tip of the chain, after which pollInterval takes effect. When
	// false, pollInterval is applied between every iteration regardless of chain position. By
	// default, false.
	tipOnly bool

	// maxRetries is the maximum number of attempts to fetch (and parse) a block before giving
	// up and shutting down the worker. -1 denotes indefinitely. By default, the first failure
	// is treated as fatal.
	maxRetries int64

	// retryInterval specifies how long the worker waits between two consecutive retry attempts.
	// By default, 2000 milliseconds.
	retryInterval uint64

	// withTxs specifies whether blocks should be fetched with full transaction objects or only
	// transaction hashes. When true, each block will contain complete transaction objects. When
	// false, only transaction hashes are included. By default, false.
	withTxs bool
}

// NewBlockWorker constructs a new [BlockWorker] instance. None of the required parameters can
// be nil. ctrlCh should be a buffered channel with a capacity of 1. processBlockFn is a callback
// invoked once for each fetched block, see [BlockWorker.processBlockFn] for details.
//
// The following optional configurations are available (see their documentation for details):
//  1. WithLogger (default: the process-wide slog logger)
//  2. WithID (default: 0)
//  3. WithRetry (default: first failure is treated as fatal)
//  4. WithStartBlock (default: 0)
//  5. WithLastBlock (default: run indefinitely)
//  6. WithPollInterval (default: 2000 milliseconds)
//  7. WithTipOnly (default: false)
//  8. WithFullTransactions (default: false)
func NewBlockWorker(
	client rpcClient,
	processBlockFn func(block *types.Block) error,
	ctrlCh <-chan struct{},
	doneCh chan<- struct{},
	errCh chan<- error,
	opts ...BlockWorkerOption) (*BlockWorker, error) {
	switch {
	case client == nil:
		return nil, fmt.Errorf("client cannot be nil")
	case processBlockFn == nil:
		return nil, fmt.Errorf("processBlock function cannot be nil")
	case ctrlCh == nil:
		return nil, fmt.Errorf("control channel cannot be nil")
	case doneCh == nil:
		return nil, fmt.Errorf("done channel cannot be nil")
	case errCh == nil:
		return nil, fmt.Errorf("error channel cannot be nil")
	}

	worker := &BlockWorker{
		// Never nil: components fall back to the process default so a syncer started
		// without WithLogger still reports what it is doing.
		logger: slog.Default(),

		client:         client,
		processBlockFn: processBlockFn,
		ctrlCh:         ctrlCh,
		doneCh:         doneCh,
		errCh:          errCh,
		maxRetries:     1,
		retryInterval:  2000,
		pollInterval:   2000,
	}

	for _, o := range opts {
		if err := o(worker); err != nil {
			return nil, err
		}
	}

	return worker, nil
}

// Start starts the worker in a goroutine. It fetches blocks sequentially starting from
// [BlockWorker.startBlock] up to [BlockWorker.lastBlock] (potentially indefinitely if not
// set), processing each one via [BlockWorker.processBlockFn]. The worker can be controlled
// through [BlockWorker.ctrlCh]. Once all assigned blocks have been processed, the worker
// sends signal to [BlockWorker.doneCh]. A value sent to [BlockWorker.errCh] indicates that
// the worker has already shut down.
func (w *BlockWorker) Start() error {
	if w.client == nil {
		return fmt.Errorf(
			"method must be invoked on an instance initialized through [NewBlockWorker]")
	}

	currentBlockNumber := w.startBlock

	// waitFn waits for the given interval while remaining responsive to control signals
	// from ctrlCh. Returns true if the block worker should shut down (ctrlCh closed),
	// false otherwise.
	waitFn := func(interval time.Duration) bool {
		if interval == 0 {
			select {
			case _, ok := <-w.ctrlCh:
				if !ok {
					return true
				}

				w.log("paused")

				_, ok = <-w.ctrlCh
				if !ok {
					return true
				}

				w.log("resume")
			default:
			}

			return false
		}

		select {
		case _, ok := <-w.ctrlCh:
			if !ok {
				return true
			}

			w.log("paused")

			_, ok = <-w.ctrlCh
			if !ok {
				return true
			}

			w.log("resume")
		case <-time.After(interval):
		}

		return false
	}

	go func() {
		if w.lastBlock != nil {
			w.log("started [%v, %v], polling every %v ms",
				w.startBlock,
				*w.lastBlock,
				w.pollInterval)
		} else {
			w.log("started [%v, +∞), polling every %v ms",
				w.startBlock,
				w.pollInterval)
		}

	break_for:
		for {
			// One span per block fetch. Without it the InstrumentedRPCClient spans below
			// are orphaned roots, and this worker's log lines carry no trace ID.
			ctx, span := tracing.Tracer().Start(context.Background(), "block.fetch",
				trace.WithAttributes(attribute.Int64("block.number", int64(currentBlockNumber))))

			w.logCtx(ctx, "fetching block %v", currentBlockNumber)

			var block *types.Block

			interval := time.Duration(w.retryInterval) * time.Millisecond

			for i := int64(1); ; i++ {
				var raw json.RawMessage

				if err := w.client.CallContext(
					ctx,
					&raw,
					"eth_getBlockByNumber",
					hexutil.EncodeBig(new(big.Int).SetUint64(currentBlockNumber)),
					w.withTxs,
				); err != nil {
					w.logWarnCtx(ctx, "RPC call failed: %v", err)

					// If [BlockWorker.maxRetries] is -1, retry indefinitely.
					if i == w.maxRetries {
						w.logErrCtx(ctx, "giving up...")
						span.SetStatus(codes.Error, "block fetch failed")
						span.End()

						w.shutDown(fmt.Errorf("cannot execute RPC call: %w", err))

						return
					}

					if waitFn(interval) {
						span.End()

						break break_for
					}

					continue
				}

				parsedBlock, err := ParseRawBlock(raw)
				if err != nil {
					w.logErrCtx(ctx, "cannot parse block: %v", err)

					// If [BlockWorker.maxRetries] is -1, retry indefinitely.
					if i == w.maxRetries {
						w.logErrCtx(ctx, "giving up...")
						span.SetStatus(codes.Error, "block parse failed")
						span.End()

						w.shutDown(fmt.Errorf("cannot parse block %d: %w", currentBlockNumber, err))

						return
					}

					if waitFn(interval) {
						span.End()

						break break_for
					}

					continue
				}

				block = parsedBlock

				break
			}

			// The fetch is done; block processing downstream gets its own trace.
			span.End()

			interval = time.Duration(w.pollInterval) * time.Millisecond

			// Block is nil when we reach the tip of the chain.
			if block != nil {
				w.log("block %v has %v txs", currentBlockNumber, len(block.Transactions))

				if err := w.processBlockFn(block); err != nil {
					w.shutDown(fmt.Errorf("cannot process block %d: %w", currentBlockNumber, err))

					return
				}

				w.log("block %v processed", currentBlockNumber)

				if w.lastBlock != nil && currentBlockNumber == *w.lastBlock {
					w.log("all blocks processed")

					break break_for
				}

				currentBlockNumber++

				// When tipOnly is true, we run without delay until we reach the tip of the
				// chain. Once there, block will be nil and pollInterval will take effect on
				// the next iteration.
				if w.tipOnly {
					interval = time.Duration(0)
				}
			}

			if waitFn(interval) {
				break break_for
			}
		}

		w.shutDown(nil)

		w.doneCh <- struct{}{}
	}()

	return nil
}

// ParseRawBlock decodes a raw JSON message into a structured [types.Block] object. If the input
// is the JSON literal "null", it gracefully returns a nil block and no error.
func ParseRawBlock(raw json.RawMessage) (*types.Block, error) {
	// A null response means the block has not been mined yet - we are at the tip.
	if string(raw) == "null" {
		return nil, nil
	}

	var block types.Block
	if err := json.Unmarshal(raw, &block); err != nil {
		return nil, err
	}

	return &block, nil
}

// shutDown gracefully shuts down the worker. If err is non-nil, it is sent to [BlockWorker.errCh].
func (w *BlockWorker) shutDown(err error) {
	if err != nil {
		w.logErr("%s", err.Error())

		w.errCh <- err
	}

	w.log("shut down")
}

// log records a lifecycle event at info level. Prefer [BlockWorker.logCtx] wherever a
// context is in scope: it adds the trace and span IDs that join the line to its trace.
func (w *BlockWorker) log(msg string, args ...any) {
	w.emit(context.Background(), slog.LevelInfo, msg, args...)
}

// logCtx records a lifecycle event at info level, tagged with the active span's IDs.
func (w *BlockWorker) logCtx(ctx context.Context, msg string, args ...any) {
	w.emit(ctx, slog.LevelInfo, msg, args...)
}

// logWarn reports a recoverable problem at warn level: something failed but the
// component is retrying or degrading rather than stopping.
func (w *BlockWorker) logWarn(msg string, args ...any) {
	w.emit(context.Background(), slog.LevelWarn, msg, args...)
}

// logWarnCtx reports a recoverable problem at warn level, tagged with the span's IDs.
func (w *BlockWorker) logWarnCtx(ctx context.Context, msg string, args ...any) {
	w.emit(ctx, slog.LevelWarn, msg, args...)
}

// logErr reports a failure at error level. Failures must not be logged through [BlockWorker.log]:
// info is filtered out in normal operation, which would make the process fail silently.
func (w *BlockWorker) logErr(msg string, args ...any) {
	w.emit(context.Background(), slog.LevelError, msg, args...)
}

// logErrCtx reports a failure at error level, tagged with the active span's IDs.
func (w *BlockWorker) logErrCtx(ctx context.Context, msg string, args ...any) {
	w.emit(ctx, slog.LevelError, msg, args...)
}

// emit is the single place that formats a message and attaches the component identity
// and trace correlation fields. The level check short-circuits before the Sprintf, which
// matters because every call site formats eagerly.
func (w *BlockWorker) emit(ctx context.Context, level slog.Level, msg string, args ...any) {
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
