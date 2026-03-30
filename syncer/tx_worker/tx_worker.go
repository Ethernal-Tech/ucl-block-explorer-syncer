package txworker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/rpc"
)

// TxJob represents a unit of work assigned to a [TxWorker], defining the range of transactions
// within a block that the worker is responsible for fetching and processing.
type TxJob struct {
	// Block is the block whose transactions are being processed.
	Block *types.Block

	// From is the index of the first tx in the block that the worker should process (inclusive).
	From uint64

	// To is the index of the last tx in the block that the worker should process (exclusive).
	To uint64
}

// TxWorker is a long-lived worker that receives jobs through the [TxWorker.jobCh], processes the
// assigned job (fetching transactions and/or their receipts for a given range within a block),
// and signals completion by writing to [TxWorker.doneCh]. For details on work distribution, see
// [CreateJobs] within helper package.
type TxWorker struct {
	// client is the EVM-based RPC client used to fetch transactions and their receipts.
	client *rpc.Client

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

	// jobCh is the channel through which the worker receives jobs. Closing this channel signals
	// the worker to stop processing and shut down gracefully.
	jobCh <-chan TxJob

	// errCh is the channel on which the worker sends an error upon encountering a fatal failure.
	// A value sent to this channel indicates that the worker has already shut down gracefully.
	errCh chan<- struct {
		Err error
		Id  uint64
	}

	// Optional fields (settable through [NewTxWorker] constructor function):

	// logger records state changes and actions during the syncer's lifecycle. By default, no
	// logging is performed.
	logger types.Logger

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
//  1. WithLogger (default: no logging)
//  2. WithID (default: 0)
//  3. WithRetry (default: first failure is treated as fatal)
//  4. WithBatchSize (default, 1)
func NewTxWorker(
	client *rpc.Client,
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
		client:        client,
		processTxsFn:  processTxsFn,
		fetchTxDataFn: fetchTxDataFn,
		doneCh:        doneCh,
		jobCh:         jobCh,
		errCh:         errCh,
		maxRetries:    1,
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

		for job := range w.jobCh {
			txs := job.Block.Transactions[job.From:job.To]

			w.log("job [%v-%v] received", job.From, job.To)

			for _, tx := range txs {
				// Fetch full transaction data only if fetchTxDataFn approves.
				if w.fetchTxDataFn(tx.Hash) {
					if err := w.fetchBatch(&batch, rpc.BatchElem{
						Method: "eth_getTransactionByHash",
						Args:   []any{tx.Hash},
						Result: tx,
					}); err != nil {
						w.shutDown(err)

						return
					}
				}

				// Transaction receipt is always fetched.
				if err := w.fetchBatch(&batch, rpc.BatchElem{
					Method: "eth_getTransactionReceipt",
					Args:   []any{tx.Hash},
					Result: tx,
				}); err != nil {
					w.shutDown(err)

					return
				}
			}

			// Flush any remaining batch elements that didn't reach batchSize.
			if len(batch) > 0 {
				if err := w.sendBatch(batch); err != nil {
					w.shutDown(err)

					return
				}

				batch = batch[:0]
			}

			if err := w.processTxsFn(txs); err != nil {
				w.shutDown(fmt.Errorf("cannot process transactions: %w", err))

				return
			}

			w.doneCh <- w.id

			w.log("job processed")
		}

		w.shutDown(nil)

		w.doneCh <- w.id
	}()

	return nil
}

// shutDown gracefully shuts down the worker. If err is non-nil, it is sent to [TxWorker.errCh].
func (w *TxWorker) shutDown(err error) {
	if err != nil {
		w.log(err.Error())

		w.errCh <- struct {
			Err error
			Id  uint64
		}{err, w.id}
	}

	w.log("shut down")
}

func (w *TxWorker) log(str string, args ...any) {
	if w.logger != nil {
		w.logger.Log(fmt.Sprintf("%s [tx worker - %v] %s",
			time.Now().Format("15:04:05.000"),
			w.id,
			fmt.Sprintf(str, args...)))
	}
}

// fetchBatch appends element to the batch and sends it once it reaches [TxWorker.batchSize].
func (w *TxWorker) fetchBatch(batch *[]rpc.BatchElem, elem rpc.BatchElem) error {
	*batch = append(*batch, elem)

	if uint64(len(*batch)) < w.batchSize {
		return nil
	}

	if err := w.sendBatch(*batch); err != nil {
		return err
	}

	*batch = (*batch)[:0]

	return nil
}

// sendBatch executes the batch RPC call, retrying on transport failure or per-element JSON-RPC
// errors up to maxRetries attempts (same semantics as block worker retry loops).
func (w *TxWorker) sendBatch(batch []rpc.BatchElem) error {
	for i := int64(1); ; i++ {
		if err := w.client.BatchCallContext(context.TODO(), batch); err != nil {
			w.log("(batch) RPC call failed: %v", err)

			// If [TxWorker.maxRetries] is -1, retry indefinitely.
			if i == w.maxRetries {
				w.log("giving up...")

				return fmt.Errorf("cannot execute (batch) RPC call: %w", err)
			}

			time.Sleep(time.Duration(w.retryInterval))

			continue
		}

		var elemErrs []error
		for _, elem := range batch {
			if elem.Error != nil {
				elemErrs = append(elemErrs, fmt.Errorf("%s for %v: %w", elem.Method, elem.Args[0], elem.Error))
			}
		}

		if len(elemErrs) > 0 {
			combined := errors.Join(elemErrs...)
			w.log("(batch) RPC sub-calls failed: %v", combined)

			if i == w.maxRetries {
				w.log("giving up...")

				return combined
			}

			time.Sleep(time.Duration(w.retryInterval))

			continue
		}

		return nil
	}
}
