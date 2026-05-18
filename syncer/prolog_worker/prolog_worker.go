package prologworker

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
)

// PrologWorker is a long-lived worker that sequentially retrieves blocks starting from
// [PrologWorker.startBlock] and processes their logs. For each block, it retrieves block
// data via [PrologWorker.getBlockFn], filters the logs according to [PrologWorker.filter],
// and invokes [PrologWorker.processLogsFn] with the matching logs. It signals completion by
// writing to [PrologWorker.doneCh] - potentially never, if [PrologWorker.lastBlock] is not
// set. It can be paused and resumed via [PrologWorker.ctrlCh].
type PrologWorker struct {
	// getBlockFn is a callback invoked by the worker when it is time to process the logs of
	// a given block. If the function returns nil, the worker interprets this as the block not
	// yet being available and waits for the duration specified by [PrologWorker.processInterval]
	// before retrying. The function is not concurrently safe - once a block object is passed
	// to the worker, it must not be accessed externally until [PrologWorker.processLogsFn] has
	// been called for it. Returning an error from this function causes the worker to stop and
	// report the error via [PrologWorker.errCh].
	getBlockFn func(blockNum uint64) (*types.Block, error)

	// processLogsFn is a callback invoked for each processed block. The first argument is the
	// unmodified block object previously returned by [PrologWorker.getBlockFn], and the second
	// is the list of logs filtered according to [PrologWorker.filter]. Returning an error from
	// this function causes the worker to stop and report the error via [PrologWorker.errCh].
	processLogsFn func(block *types.Block, logs []*types.ReceiptLog) error

	// filter specifies which logs to process, keyed by contract address. For each address, the
	// value is a set of topics - a log is included if at least one of its topics matches any
	// entry in the set. If the topic set for a given address is nil, all logs emitted by that
	// address are included. If filter itself is nil, all logs in every block are processed. This
	// field is required to ensure the caller explicitly acknowledges the filtering behavior.
	filter map[string]map[string]struct{}

	// ctrlCh is the channel through which an external caller can pause and resume the worker.
	// Sending a signal pauses the worker, sending another resumes it, alternating with each
	// signal. When the channel is closed (and all previous signals are processed), the worker
	// shuts down gracefully and sends a signal to [PrologWorker.doneCh].
	ctrlCh <-chan struct{}

	// doneCh is the channel on which the worker sends a signal upon completing its work (by
	// reaching [PrologWorker.lastBlock]), or when it has finished shutting down gracefully due
	// to [PrologWorker.ctrlCh] being closed.
	doneCh chan<- string

	// errCh is the channel on which the worker sends an error upon encountering a fatal failure.
	// A value sent to this channel indicates that the worker has already shut down gracefully.
	errCh chan<- struct {
		Err error
		Id  string
	}

	// Optional fields (settable through [NewPrologWorker] constructor function):

	// logger records state changes and actions during the syncer's lifecycle. By default, no
	// logging is performed.
	logger types.Logger

	// id is the unique identifier of the worker. By default, it is set to pseudo-random id.
	id string

	// startBlock is the block number from which the worker begins processing. By default, 0.
	startBlock uint64

	// lastBlock is the last block the worker will process, inclusive. Once reached, the worker
	// shuts down gracefully. By default, not set, meaning the worker runs indefinitely.
	//
	// Note: it is a pointer so that a value of zero (0) can be distinguished from the field not
	// being set at all.
	lastBlock *uint64

	// processInterval specifies how often the worker attempts to process new blocks. By default,
	// 2000 milliseconds.
	processInterval uint64

	// waitOnlyOnNil controls when [PrologWorker.processInterval] is applied. When true, it is
	// applied only when [PrologWorker.getBlockFn] returns nil. By default, false.
	waitOnlyOnNil bool
}

// NewPrologWorker constructs a new [PrologWorker] instance. None of the required parameters can
// be nil, except for filter which must be explicitly set to nil to indicate that all logs should
// be processed. ctrlCh should be a buffered channel with a capacity of 1. Functions getBlockFn
// and processLogsFn are callbacks invoked for each block, see [PrologWorker.getBlockFn] and
// [PrologWorker.processLogsFn] for details.
//
// filter specifies which logs to process, keyed by contract address. For each address, the value
// is a list of topics - a log is included if at least one of its topics matches any entry in the
// list. If the topic list for a given address is nil, all logs emitted by that address are included.
// If filter itself is nil, all logs in every block are processed.
//
// The following optional configurations are available (see their documentation for details):
//  1. WithLogger (default: no logging)
//  2. WithID (default: pseudo-random string)
//  3. WithStartBlock (default: 0)
//  4. WithLastBlock (default: run indefinitely)
//  6. WithProcessInterval (default: 2000 milliseconds)
//  7. WithWaitOnlyOnNil (default: false)
func NewPrologWorker(
	getBlockFn func(blockNum uint64) (*types.Block, error),
	processLogsFn func(block *types.Block, logs []*types.ReceiptLog) error,
	filter map[string][]string,
	ctrlCh <-chan struct{},
	doneCh chan<- string,
	errCh chan<- struct {
		Err error
		Id  string
	},
	opts ...PrologWorkerOption) (*PrologWorker, error) {
	switch {
	case getBlockFn == nil:
		return nil, fmt.Errorf("getBlock function cannot be nil")
	case processLogsFn == nil:
		return nil, fmt.Errorf("processBlock function cannot be nil")
	case ctrlCh == nil:
		return nil, fmt.Errorf("control channel cannot be nil")
	case doneCh == nil:
		return nil, fmt.Errorf("done channel cannot be nil")
	case errCh == nil:
		return nil, fmt.Errorf("error channel cannot be nil")
	}

	worker := &PrologWorker{
		getBlockFn:      getBlockFn,
		processLogsFn:   processLogsFn,
		ctrlCh:          ctrlCh,
		doneCh:          doneCh,
		errCh:           errCh,
		processInterval: 2000,
		id:              fmt.Sprintf("%x", rand.Uint64()),
	}

	if filter != nil {
		worker.filter = map[string]map[string]struct{}{}

		for addr, topics := range filter {
			if topics == nil {
				worker.filter[addr] = nil

				continue
			}

			worker.filter[addr] = map[string]struct{}{}

			for _, topic := range topics {
				worker.filter[addr][topic] = struct{}{}
			}
		}
	}

	for _, o := range opts {
		if err := o(worker); err != nil {
			return nil, err
		}
	}

	return worker, nil
}

// Start starts the worker in a goroutine. It retrieves blocks sequentially starting from
// [PrologWorker.startBlock] up to [PrologWorker.lastBlock] (potentially indefinitely if not
// set) via [PrologWorker.getBlockFn]. For each retrieved block, logs are filtered according
// to [PrologWorker.filter], and those that pass are forwarded to [PrologWorker.processLogsFn].
// The worker can be controlled through [PrologWorker.ctrlCh]. Once all assigned blocks have
// been processed, the worker sends a signal to [PrologWorker.doneCh]. A value sent to
// [PrologWorker.errCh] indicates that the worker has already shut down.
func (w *PrologWorker) Start() error {
	if w.getBlockFn == nil {
		return fmt.Errorf(
			"method must be invoked on an instance initialized through [NewPrologWorker]")
	}

	currentBlock := w.startBlock

	go func() {
		if w.lastBlock != nil {
			w.log("started [%v, %v], processing every %v ms",
				w.startBlock,
				*w.lastBlock,
				w.processInterval)
		} else {
			w.log("started [%v, +∞), processing every %v ms",
				w.startBlock,
				w.processInterval)
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

			w.log("processing block %v", currentBlock)

			block, err := w.getBlockFn(currentBlock)

			switch {
			case err != nil:
				w.shutDown(fmt.Errorf("cannot get block %d: %w", currentBlock, err))

				return
			case block != nil:
				filteredLogs := []*types.ReceiptLog{}

				for _, tx := range block.Transactions {
					for _, log := range tx.Logs {
						if w.filter == nil {
							filteredLogs = append(filteredLogs, &log)

							continue
						}

						topics, ok := w.filter[log.Address]
						if !ok {
							continue
						}

						for _, topic := range log.Topics {
							if _, ok := topics[topic]; ok {
								filteredLogs = append(filteredLogs, &log)

								break
							}
						}
					}

					w.log("found %d logs in tx %s", len(filteredLogs), tx.Hash)
				}

				w.log("found a total of %d logs", len(filteredLogs))

				if err := w.processLogsFn(block, filteredLogs); err != nil {
					w.shutDown(
						fmt.Errorf("cannot process logs for block %d: %w", currentBlock, err))

					return
				}

				w.log("logs in block %v processed", currentBlock)

				if w.lastBlock != nil && currentBlock == *w.lastBlock {
					w.log("all blocks processed")

					break break_for
				}

				currentBlock++

				if !w.waitOnlyOnNil {
					time.Sleep(time.Duration(w.processInterval) * time.Millisecond)
				}

			default:
				// TODO: handle control signals from ctrlCh during sleep

				// No block available yet (perhaps we reached the tip of the chain).
				// Wait before trying again.
				time.Sleep(time.Duration(w.processInterval) * time.Millisecond)
			}
		}

		w.shutDown(nil)

		w.doneCh <- w.id
	}()

	return nil
}

// shutDown gracefully shuts down the worker. If err is non-nil, it is sent to [PrologWorker.errCh].
func (w *PrologWorker) shutDown(err error) {
	if err != nil {
		w.log("%s", err.Error())

		w.errCh <- struct {
			Err error
			Id  string
		}{err, w.id}
	}

	w.log("shut down")
}

func (w *PrologWorker) log(str string, args ...any) {
	if w.logger != nil {
		w.logger.Log(fmt.Sprintf("%s [prolog worker - %v] %s",
			time.Now().Format("15:04:05.000"),
			w.id,
			fmt.Sprintf(str, args...)))
	}
}
