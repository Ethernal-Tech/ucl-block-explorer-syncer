package syncer

import (
	"fmt"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
)

type SyncerOption func(*Syncer) error

// WithLogger configures the syncer to log its state changes and actions during its lifecycle.
// By default, no logging is performed. You can use [helper.DefaultLogger] to log to standard
// output using fmt formatting.
func WithLogger(logger types.Logger) SyncerOption {
	return func(s *Syncer) error {
		s.logger = logger

		return nil
	}
}

// WithBlockWorkerStartBlock sets the block number from which the block worker begins processing.
// By default, 0.
func WithBlockWorkerStartBlock(block uint64) SyncerOption {
	return func(s *Syncer) error {
		s.startBlockBW = block

		return nil
	}
}

// WithTransactionWorkerStartBlock sets the block number from which the transaction workers begin
// processing. By default, 0.
func WithTransactionkWorkerStartBlock(block uint64) SyncerOption {
	return func(s *Syncer) error {
		s.startBlockTW = block

		return nil
	}
}

// WithPollInterval configures the syncer to use the provided polling interval for fetching
// blocks, expressed in milliseconds. The interval must be between 200 and 900000 milliseconds,
// inclusive. By default, the polling interval is set to 2000 milliseconds.
func WithPollInterval(pollInterval uint64) SyncerOption {
	return func(s *Syncer) error {
		if pollInterval < 200 {
			return fmt.Errorf("poll interval must be at least 200 milliseconds")
		} else if pollInterval > 900000 {
			return fmt.Errorf("poll interval must not exceed 900000 milliseconds (15 minutes)")
		}

		s.pollInterval = pollInterval

		return nil
	}
}

// WithTipOnly configures the syncer to apply the poll interval only after it has caught up with
// the tip of the chain. Until then, the syncer processes blocks (and transactions) as fast as
// possible without any delay. By default, the poll interval is applied between every iteration
// regardless of the syncer's position relative to the chain tip.
func WithTipOnly() SyncerOption {
	return func(s *Syncer) error {
		s.tipOnly = true

		return nil
	}
}

// WithTxPool configures the syncer to also fetch pending transactions from the transaction pool
// via the txpool_content RPC method. The argument specifies how often the transaction pool is
// fetched, expressed in milliseconds. The interval must be between 200 and 900000 milliseconds,
// inclusive. By default, the transaction pool is not fetched.
func WithTxPool(pollInterval uint64) SyncerOption {
	return func(s *Syncer) error {
		if pollInterval < 200 {
			return fmt.Errorf("pool interval must be at least 200 milliseconds")
		} else if pollInterval > 900000 {
			return fmt.Errorf("pool interval must not exceed 900000 milliseconds (15 minutes)")
		}

		s.syncTxPool = true
		s.txPoolPollInterval = pollInterval

		return nil
	}
}

// WithFullTransactions configures the syncer to fetch full transaction objects for each block
// instead of only transaction hashes. By default, only transaction hashes are included.
func WithFullTransactions() SyncerOption {
	return func(s *Syncer) error {
		s.withTxs = true

		return nil
	}
}

// WithRetry configures the maximum number of RPC retry attempts for fetching blockchain data
// (blocks, transactions, and receipts) before giving up. It also configures the interval between
// them. The maxRetries must be between 1 and 500, or -1 to retry indefinitely. The retryInterval
// is in milliseconds and must be between 200 and 900000 (15 minutes). By default, the first
// failure is treated as fatal.
func WithRetry(maxRetries int64, retryInterval uint64) SyncerOption {
	return func(s *Syncer) error {
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

// WithBatchSize configures the transaction workers to group RPC calls into batches of the given
// size instead of sending each call individually. By default, 1, meaning no real batching occurs.
// The batchSize must be between 1 and 500.
func WithBatchSize(batchSize uint64) SyncerOption {
	return func(s *Syncer) error {
		if batchSize == 0 || batchSize > 500 {
			return fmt.Errorf("batchSize must be between 1 and 500")
		}

		s.batchSize = batchSize

		return nil
	}
}

// WithMaxTxWorkers sets the maximum number of transaction workers that can be active at a time.
// By default, 1.
func WithMaxTxWorkers(maxTxWorkers uint64) SyncerOption {
	return func(s *Syncer) error {
		if maxTxWorkers == 0 {
			return fmt.Errorf("maxTxWorkers must be greater than 0")
		}

		s.maxTxWorkers = maxTxWorkers

		return nil
	}
}

// WithErc20Stats configures the syncer to also track statistics for various ERC-20 tokens using
// the provided backend. For additional information on the underlying aggregation process, see
// the [Erc20Backend] interface documentation.
func WithErc20Stats(backend Erc20Backend) SyncerOption {
	return func(s *Syncer) error {
		if backend == nil {
			return fmt.Errorf("erc20 backend must be provided")
		}

		s.erc20Backend = backend

		return nil
	}
}

// WithErc20WatchlistCheckInterval sets how often the syncer checks the ERC-20 watchlist for changes,
// expressed in milliseconds. The interval must be between 200 and 900000 milliseconds, inclusive.
// By default, 2000 milliseconds.
func WithErc20WatchlistCheckInterval(interval uint64) SyncerOption {
	return func(s *Syncer) error {
		if interval < 200 || interval > 900000 {
			return fmt.Errorf("watchlist check interval must be between 200ms and 15 minutes")
		}

		s.erc20WatchlistCheckInterval = interval

		return nil
	}
}

// WithErc20StartFromTip configures how the syncer initializes processing for ERC-20 tokens newly
// added to the watchlist. By default (false), the syncer starts from the last processed block
// for that token, or from block 0 if it has never been processed before. When set to true, the
// syncer ignores historical data and starts processing from the current chain tip.
func WithErc20StartFromTip() SyncerOption {
	return func(s *Syncer) error {
		s.erc20StartFromTip = true

		return nil
	}
}

// WithErc20ProcessInterval sets the delay between attempts to process new blocks for ERC-20
// events when the requested block is not yet available, expressed in milliseconds. The interval
// must be between 200 and 900000 milliseconds, inclusive. By default, 2000 milliseconds.
func WithErc20ProcessInterval(interval uint64) SyncerOption {
	return func(s *Syncer) error {
		if interval < 200 || interval > 900000 {
			return fmt.Errorf("erc20 process interval must be between 200ms and 15 minutes")
		}

		s.erc20ProcessInterval = interval

		return nil
	}
}

// WithEoaActivityStats configures the syncer to track EOA activity statistics using the provided
// backend. For additional information on the underlying process, see the [EoaActivityBackend]
// interface documentation.
func WithEoaActivityStats(backend EoaActivityBackend) SyncerOption {
	return func(s *Syncer) error {
		if backend == nil {
			return fmt.Errorf("eoa activity backend must be provided")
		}

		s.eoaActivityBackend = backend

		return nil
	}
}

// WithEoaActivityProcessInterval sets the delay between retries when the requested block is
// not yet available for EOA activity processing, expressed in milliseconds. The interval must
// be between 200 and 900000 milliseconds, inclusive. By default, 2000 milliseconds.
func WithEoaActivityProcessInterval(interval uint64) SyncerOption {
	return func(s *Syncer) error {
		if interval < 200 || interval > 900000 {
			return fmt.Errorf("eoa activity process interval must be between 200ms and 15 minutes")
		}

		s.eoaActivityProcessInterval = interval

		return nil
	}
}

// WithEoaActivityStartBlock sets the block number from which the syncer begins processing EOA
// activities. By default, 0.
func WithEoaActivityStartBlock(block uint64) SyncerOption {
	return func(s *Syncer) error {
		s.eoaActivityStartBlock = block

		return nil
	}
}

// WithEsgAggregationStats configures the syncer to track ESG aggregation statistics using the provided
// backend. For additional information on the underlying process, see the [ESGAggregationBackend]
// interface documentation.
func WithEsgAggregationStats(backend ESGAggregationBackend) SyncerOption {
	return func(s *Syncer) error {
		if backend == nil {
			return fmt.Errorf("esg aggregation backend must be provided")
		}

		s.esgAggregationBackend = backend

		return nil
	}
}

// WithESGAggregationPollInterval sets the delay between two ESG consequtive retrievals.
// The interval must be at least 200 milliseconds. By default, 24 hours.
func WithESGAggregationPollInterval(interval uint64) SyncerOption {
	return func(s *Syncer) error {
		if interval < 200 {
			return fmt.Errorf("esg aggregation process interval must be at least 200ms")
		}

		s.esgAggregationPollInterval = interval

		return nil
	}
}
