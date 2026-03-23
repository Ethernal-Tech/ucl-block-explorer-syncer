package blockworker

import (
	"fmt"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
)

type BlockWorkerOption func(*BlockWorker) error

// WithLogger configures the block worker to log its state changes and actions during its
// lifecycle. By default, no logging is performed. You can use [helper.DefaultLogger] to log
// to standard output using fmt formatting.
func WithLogger(logger types.Logger) BlockWorkerOption {
	return func(w *BlockWorker) error {
		w.logger = logger

		return nil
	}
}

// WithID sets the unique identifier of the worker. By default, the ID is 0.
func WithID(id uint64) BlockWorkerOption {
	return func(w *BlockWorker) error {
		w.id = id

		return nil
	}
}

// WithRetry configures the maximum number of RPC retry attempts and the interval between them
// for fetching a block. The maxRetries must be between 1 and 500, or -1 to retry indefinitely.
// The retryInterval is in milliseconds and must be between 200 and 900000 (15 minutes). By
// default, the first failure is treated as fatal.
func WithRetry(maxRetries int64, retryInterval uint64) BlockWorkerOption {
	return func(s *BlockWorker) error {
		switch {
		case maxRetries < -1 || maxRetries == 0 || maxRetries > 500:
			return fmt.Errorf("maxRetries must be between 1 and 500, or -1 for indefinite retries")
		case retryInterval < 200 || retryInterval > 900000:
			return fmt.Errorf("retryInterval must be between 200ms and 15 minutes (900000ms)")

		}

		s.maxRetries = maxRetries
		s.retryInterval = retryInterval

		return nil
	}
}

// WithStartBlock sets the block number from which the worker begins processing. By default, 0.
func WithStartBlock(block uint64) BlockWorkerOption {
	return func(w *BlockWorker) error {
		if w.lastBlock != nil && block > *w.lastBlock {
			return fmt.Errorf("startBlock (%d) must be less than or equal to lastBlock (%d)",
				block,
				*w.lastBlock)
		}

		w.startBlock = block

		return nil
	}
}

// WithLastBlock sets the last block the block worker will process, inclusive. Once the worker
// processes this block, it shuts down gracefully. By default, worker runs indefinitely.
func WithLastBlock(block uint64) BlockWorkerOption {
	return func(w *BlockWorker) error {
		if block < w.startBlock {
			return fmt.Errorf("lastBlock (%d) must be greater than or equal to startBlock (%d)",
				block,
				w.startBlock)
		}

		w.lastBlock = &block

		return nil
	}
}

// WithPollInterval configures the block worker to use the provided polling interval, expressed
// in milliseconds. The interval must be between 200 and 900000 milliseconds, inclusive. By default,
// the polling interval is set to 2000 milliseconds.
func WithPollInterval(pollInterval uint64) BlockWorkerOption {
	return func(w *BlockWorker) error {
		if pollInterval < 200 {
			return fmt.Errorf("poll interval must be at least 200 milliseconds")
		} else if pollInterval > 900000 {
			return fmt.Errorf("poll interval must not exceed 900000 milliseconds (15 minutes)")
		}

		w.pollInterval = pollInterval

		return nil
	}
}

// WithTipOnly configures the block worker to apply the poll interval only after it has caught up
// with the tip of the chain. Until then, the worker processes blocks (and transactions) as fast
// as possible without any delay. By default, the poll interval is applied between every iteration
// regardless of the worker's position relative to the chain tip.
func WithTipOnly() BlockWorkerOption {
	return func(w *BlockWorker) error {
		w.tipOnly = true

		return nil
	}
}

// WithFullTransactions configures the block worker to fetch full transaction objects for each
// block instead of only transaction hashes. By default, only transaction hashes are included.
func WithFullTransactions() BlockWorkerOption {
	return func(w *BlockWorker) error {
		w.withTxs = true

		return nil
	}
}
