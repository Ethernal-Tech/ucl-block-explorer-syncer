package syncer

import (
	"database/sql"
	"fmt"

	erc20worker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/erc20_worker"
	entitystatsworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/entity_stats_worker"
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

// WithErc20Stats enables asynchronous ERC-20 Transfer aggregation after each committed block.
// db must remain open for the syncer lifetime. buffer is the bounded queue depth; if zero, 64 is used.
// When the queue is full, blocks are dropped and logged (indexing never blocks).
func WithErc20Stats(db *sql.DB, buffer uint) SyncerOption {
	return func(s *Syncer) error {
		if db == nil {
			return fmt.Errorf("erc20 stats database handle cannot be nil")
		}
		if buffer == 0 {
			buffer = 64
		}

		s.erc20DB = db
		s.erc20StatsCh = make(chan erc20worker.BlockJob, buffer)

		return nil
	}
}

// WithEntityStats enables asynchronous adoption-entity stats after each committed block
// (per-day unique participants + first-seen EOA via eth_getCode). db must stay open for the
// syncer lifetime. buffer is the bounded queue depth; if zero, 64 is used.
func WithEntityStats(db *sql.DB, buffer uint) SyncerOption {
	return func(s *Syncer) error {
		if db == nil {
			return fmt.Errorf("entity stats database handle cannot be nil")
		}
		if buffer == 0 {
			buffer = 64
		}

		s.entityDB = db
		s.entityStatsCh = make(chan entitystatsworker.BlockJob, buffer)

		return nil
	}
}
