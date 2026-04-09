package txpoolworker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/rpc"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
)

// TxPoolWorker is a long-lived worker that periodically fetches the transaction pool from an
// EVM-based node. It can be paused and resumed via [TxPoolWorker.ctrlCh].
type TxPoolWorker struct {
	// client is the EVM-based RPC client used to fetch the transaction pool.
	client *rpc.Client

	// processTxPoolFn is a callback invoked on each poll with the current pending and queued
	// transactions. Returning an error from this function causes the worker to stop and report
	// the error via [TxPoolWorker.errCh].
	processTxPoolFn func(pending, queued []*types.Transaction) error

	// ctrlCh is the channel through which an external caller can pause and resume the worker.
	// Sending a signal pauses the worker, sending another resumes it, alternating with each
	// signal. When the channel is closed (and all previous signals are processed), the worker
	// shuts down gracefully and sends a signal to [TxPoolWorker.doneCh].
	ctrlCh <-chan struct{}

	// doneCh is the channel on which the worker sends a signal when it has finished shutting
	// down gracefully due to [TxPoolWorker.ctrlCh] being closed.
	doneCh chan<- struct{}

	// errCh is the channel on which the worker sends an error upon encountering a fatal failure.
	// A value sent to this channel indicates that the worker has already shut down gracefully.
	errCh chan<- error

	// Optional fields (settable through [NewTxPoolWorker] constructor function):

	// logger records state changes and actions during the worker's lifecycle. By default, no
	// logging is performed.
	logger types.Logger

	// id is the unique identifier of the worker. By default, it is set to zero.
	id uint64

	// pollInterval specifies how often the worker fetches and processes the transaction pool.
	// By default, 2000 milliseconds.
	pollInterval uint64

	// maxRetries is the maximum number of RPC request attempts for fetching the transaction pool
	// before giving up. -1 denotes indefinitely. By default, the first failure is treated as
	// fatal.
	maxRetries int64

	// retryInterval specifies how long the worker waits between two consecutive RPC attempts.
	// By default, 2000 milliseconds.
	retryInterval uint64
}

// NewTxPoolWorker constructs a new [TxPoolWorker] instance. None of the required parameters can
// be nil. ctrlCh should be a buffered channel with a capacity of 1. processTxPoolFn is a callback
// invoked once for each fetching, see [TxPoolWorker.processTxPoolFn] for details.
//
// The following optional configurations are available (see their documentation for details):
//  1. WithLogger (default: no logging)
//  2. WithID (default: 0)
//  3. WithRetry (default: first failure is treated as fatal)
//  4. WithPollInterval (default: 2000 milliseconds)
func NewTxPoolWorker(
	client *rpc.Client,
	processTxPoolFn func(pending, queued []*types.Transaction) error,
	ctrlCh <-chan struct{},
	doneCh chan<- struct{},
	errCh chan<- error,
	opts ...TxPoolWorkerOption) (*TxPoolWorker, error) {
	switch {
	case client == nil:
		return nil, fmt.Errorf("client cannot be nil")
	case processTxPoolFn == nil:
		return nil, fmt.Errorf("processTxPool function cannot be nil")
	case ctrlCh == nil:
		return nil, fmt.Errorf("control channel cannot be nil")
	case doneCh == nil:
		return nil, fmt.Errorf("done channel cannot be nil")
	case errCh == nil:
		return nil, fmt.Errorf("error channel cannot be nil")
	}

	worker := &TxPoolWorker{
		client:          client,
		processTxPoolFn: processTxPoolFn,
		ctrlCh:          ctrlCh,
		doneCh:          doneCh,
		errCh:           errCh,
		maxRetries:      1,
		retryInterval:   2000,
		pollInterval:    2000,
	}

	for _, o := range opts {
		if err := o(worker); err != nil {
			return nil, err
		}
	}

	return worker, nil
}

// Start starts the worker in a goroutine. It periodically fetches the transaction pool, that is
// pending and queued transactions, and processes them via [TxPoolWorker.processTxPoolFn]. The
// worker can be controlled through [TxPoolWorker.ctrlCh]. A value sent to [TxPoolWorker.errCh]
// indicates that the worker has already shut down.
func (w *TxPoolWorker) Start() error {
	if w.client == nil {
		return fmt.Errorf(
			"method must be invoked on an instance initialized through [NewTxPoolWorker]")
	}

	fetchFn := func() ([]*types.Transaction, []*types.Transaction, error) {
		var content struct {
			Pending map[string]map[string]json.RawMessage `json:"pending"`
			Queued  map[string]map[string]json.RawMessage `json:"queued"`
		}

		var pending, queued []*types.Transaction

		for i := int64(1); ; i++ {
			if err := w.client.CallContext(
				context.TODO(),
				&content,
				"txpool_content",
			); err != nil {
				w.log("RPC call failed: %v", err)

				// If [TxPoolWorker.maxRetries] is -1, retry indefinitely.
				if i == w.maxRetries {
					w.log("giving up...")

					return nil, nil, fmt.Errorf("cannot execute RPC call: %w", err)
				}

				time.Sleep(time.Duration(w.retryInterval))

				continue
			}

			break
		}

		for _, nonceTxMap := range content.Pending {
			for _, rawTx := range nonceTxMap {
				var tx types.Transaction

				if err := json.Unmarshal(rawTx, &tx); err != nil {
					return nil, nil, fmt.Errorf("cannot unmarshal pending transaction: %w", err)
				}

				pending = append(pending, &tx)
			}
		}

		for _, nonceTxMap := range content.Queued {
			for _, rawTx := range nonceTxMap {
				var tx types.Transaction

				if err := json.Unmarshal(rawTx, &tx); err != nil {
					return nil, nil, fmt.Errorf("cannot unmarshal queued transaction: %w", err)
				}

				queued = append(queued, &tx)
			}
		}

		return pending, queued, nil
	}

	go func() {
		w.log("started, polling every %v ms", w.pollInterval)

	break_for:
		for {
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

			w.log("fetching tx pool")

			pending, queued, err := fetchFn()
			if err != nil {
				w.shutDown(err)

				return
			}

			w.log("%v txs fetched (%d pending, %d queued)",
				len(pending)+len(queued),
				len(pending),
				len(queued),
			)

			if len(pending) > 0 || len(queued) > 0 {
				if err := w.processTxPoolFn(pending, queued); err != nil {
					w.shutDown(fmt.Errorf("cannot process txpool: %w", err))

					return
				}

				w.log("txpool processed")
			}

			time.Sleep(time.Duration(w.pollInterval) * time.Millisecond)
		}

		w.shutDown(nil)

		w.doneCh <- struct{}{}
	}()

	return nil
}

// shutDown gracefully shuts down the worker. If err is non-nil, it is sent to [TxPoolWorker.errCh].
func (w *TxPoolWorker) shutDown(err error) {
	if err != nil {
		w.log("%s", err.Error())

		w.errCh <- err
	}

	w.log("shut down")
}

func (w *TxPoolWorker) log(str string, args ...any) {
	if w.logger != nil {
		w.logger.Log(fmt.Sprintf("%s [tx pool worker - %v] %s",
			time.Now().Format("15:04:05.000"),
			w.id,
			fmt.Sprintf(str, args...)))
	}
}
