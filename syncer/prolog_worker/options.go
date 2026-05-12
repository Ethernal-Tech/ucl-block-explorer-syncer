package prologworker

import (
	"fmt"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
)

type PrologWorkerOption func(*PrologWorker) error

// WithLogger configures the prolog worker to log its state changes and actions during its
// lifecycle. By default, no logging is performed. You can use [helper.DefaultLogger] to log
// to standard output using fmt formatting.
func WithLogger(logger types.Logger) PrologWorkerOption {
	return func(w *PrologWorker) error {
		w.logger = logger

		return nil
	}
}

// WithID sets the unique identifier of the worker. By default, the ID is pseudo-random value.
func WithID(id string) PrologWorkerOption {
	return func(w *PrologWorker) error {
		w.id = id

		return nil
	}
}

// WithStartBlock sets the block number from which the worker begins processing. By default, 0.
func WithStartBlock(block uint64) PrologWorkerOption {
	return func(w *PrologWorker) error {
		if w.lastBlock != nil && block > *w.lastBlock {
			return fmt.Errorf("startBlock (%d) must be less than or equal to lastBlock (%d)",
				block,
				*w.lastBlock)
		}

		w.startBlock = block

		return nil
	}
}

// WithLastBlock sets the last block the prolog worker will process, inclusive. Once the worker
// processes this block, it shuts down gracefully. By default, worker runs indefinitely.
func WithLastBlock(block uint64) PrologWorkerOption {
	return func(w *PrologWorker) error {
		if block < w.startBlock {
			return fmt.Errorf("lastBlock (%d) must be greater than or equal to startBlock (%d)",
				block,
				w.startBlock)
		}

		w.lastBlock = &block

		return nil
	}
}

// WithProcessInterval configures the worker to use the provided process interval, expressed in
// milliseconds. The interval must be between 200 and 900000 milliseconds, inclusive. By default,
// the process interval is set to 2000 milliseconds.
func WithProcessInterval(processInterval uint64) PrologWorkerOption {
	return func(w *PrologWorker) error {
		if processInterval < 200 {
			return fmt.Errorf("process interval must be at least 200 milliseconds")
		} else if processInterval > 900000 {
			return fmt.Errorf("process interval must not exceed 900000 milliseconds (15 minutes)")
		}

		w.processInterval = processInterval

		return nil
	}
}

// WithWaitOnlyOnNil configures the prolog worker to apply the process interval only when
// [PrologWorker.getBlockFn] returns nil. Until then, the worker processes blocks as fast
// as possible without any delay. By default, the process interval is applied between every
// iteration regardless of what [PrologWorker.getBlockFn] returns.
func WithWaitOnlyOnNil() PrologWorkerOption {
	return func(w *PrologWorker) error {
		w.waitOnlyOnNil = true

		return nil
	}
}
