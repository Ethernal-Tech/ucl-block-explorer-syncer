package blockworker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

// BlockWorker is a long-lived worker that sequentially fetches and processes blocks from an
// EVM-based node, starting from [BlockWorker.startBlock]. It signals completion by writing
// to [BlockWorker.doneCh] - potentially never, if [BlockWorker.lastBlock] is not set. It can
// be paused and resumed via [BlockWorker.ctrlCh].
type BlockWorker struct {
	// client is the EVM-based RPC client used to fetch blocks.
	client *rpc.Client

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

	// logger records state changes and actions during the syncer's lifecycle. By default, no
	// logging is performed.
	logger types.Logger

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

	// maxRetries is the maximum number of RPC request attempts for fetching a block before giving
	// up. -1 denotes indefinitely. By default, the first failure is treated as fatal.
	maxRetries int64

	// retryInterval specifies how long the worker waits between two consecutive RPC attempts.
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
//  1. WithLogger (default: no logging)
//  2. WithID (default: 0)
//  3. WithRetry (default: first failure is treated as fatal)
//  4. WithStartBlock (default: 0)
//  5. WithLastBlock (default: run indefinitely)
//  6. WithPollInterval (default: 2000 milliseconds)
//  7. WithTipOnly (default: false)
//  8. WithFullTransactions (default: false)
func NewBlockWorker(
	client *rpc.Client,
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

	currentBlock := w.startBlock

	// fetchFn fetches a single block by number via RPC, retrying on failure. The following
	// returns are possible:
	// 1. nil, error - an error occurred, processing should stop.
	// 2. block, nil - a block was fetched successfully and should be processed.
	// 3. nil, nil  - the block has not been mined yet; we are at the tip of the chain.
	fetchFn := func(number uint64) (*types.Block, error) {
		var raw json.RawMessage

		for i := int64(1); ; i++ {
			if err := w.client.CallContext(
				context.TODO(),
				&raw,
				"eth_getBlockByNumber",
				hexutil.EncodeBig(new(big.Int).SetUint64(number)),
				w.withTxs,
			); err != nil {
				w.log("RPC call failed: %v", err)

				// If [BlockWorker.maxRetries] is -1, retry indefinitely.
				if i == w.maxRetries {
					w.log("giving up...")

					return nil, fmt.Errorf("cannot execute RPC call: %w", err)
				}

				time.Sleep(time.Duration(w.retryInterval))

				continue
			}

			break
		}

		// A null response means the block has not been mined yet - we are at the tip.
		if string(raw) == "null" {
			return nil, nil
		}

		var block types.Block

		if err := json.Unmarshal(raw, &block); err != nil {
			return nil, fmt.Errorf("cannot unmarshal the block %d: %w", number, err)
		}

		return &block, nil
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
			// Check if a stop signal has been received. A second receive on ctrlCh
			// waits for a resume signal; if the channel is closed, the loop exits.
			select {
			case <-w.ctrlCh:
				w.log("stop")

				_, ok := <-w.ctrlCh
				if !ok {
					break break_for
				}

				w.log("resume")
			default:
			}

			w.log("fetching block %v", currentBlock)

			block, err := fetchFn(currentBlock)

			switch {
			case err != nil:
				w.shutDown(err)

				return
			case block != nil:
				w.log("block %v has %v txs", currentBlock, len(block.Transactions))

				if err := w.processBlockFn(block); err != nil {
					w.shutDown(fmt.Errorf("cannot process block %d: %w", currentBlock, err))

					return
				}

				w.log("block %v processed", currentBlock)

				if w.lastBlock != nil && currentBlock == *w.lastBlock {
					w.log("all blocks processed")

					break break_for
				}

				currentBlock++

				// When tipOnly is true, we run without delay until we reach the tip, after
				// which pollInterval takes effect (handled in the default case).
				if !w.tipOnly {
					time.Sleep(time.Duration(w.pollInterval) * time.Millisecond)
				}

			default:
				//w.log("tip of the chain, sleep")

				// No block available yet (tip of the chain). Wait before trying again.
				time.Sleep(time.Duration(w.pollInterval) * time.Millisecond)
			}
		}

		w.shutDown(nil)

		w.doneCh <- struct{}{}
	}()

	return nil
}

// shutDown gracefully shuts down the worker. If err is non-nil, it is sent to [BlockWorker.errCh].
func (w *BlockWorker) shutDown(err error) {
	if err != nil {
		w.log("%s", err.Error())

		w.errCh <- err
	}

	w.log("shut down")
}

func (w *BlockWorker) log(str string, args ...any) {
	if w.logger != nil {
		w.logger.Log(fmt.Sprintf("%s [block worker - %v] %s",
			time.Now().Format("15:04:05.000"),
			w.id,
			fmt.Sprintf(str, args...)))
	}
}
