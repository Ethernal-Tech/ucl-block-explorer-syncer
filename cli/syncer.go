package cli

import (
	"database/sql"
	"fmt"

	eoaactivitybackend "github.com/Ethernal-Tech/ucl-block-explorer-syncer/eoa_activity_backend"
	erc20backend "github.com/Ethernal-Tech/ucl-block-explorer-syncer/erc20_backend"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/storage_handler"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/helper"
	"github.com/spf13/cobra"
)

var (
	rpcUrl                      string
	connString                  string
	logging                     bool
	pollInterval                uint64
	tipOnly                     bool
	syncTxPool                  bool
	txPoolPollInterval          uint64
	fullBlock                   bool
	batchSize                   uint64
	txWorkers                   uint64
	erc20Stats                  bool
	erc20WatchlistCheckInterval uint64
	erc20StartFromTip           bool
	erc20ProcessInterval        uint64
	eoaActivityStats            bool
	eoaActivityProcessInterval  uint64
	circulationPollInterval     uint64
)

var syncerCommand = &cobra.Command{
	Use:     "sync",
	Aliases: []string{"syncer"},
	Short:   "Indexes blocks and transactions from an EVM-compatible node into PostgreSQL.",
	Long:    doc,
	RunE:    execute,
}

func setRequiredFlags() {
	syncerCommand.Flags().StringVarP(&rpcUrl, "rpc-url", "r", "",
		"[REQUIRED] JSON-RPC endpoint URL of the node to sync from")
	syncerCommand.MarkFlagRequired("rpc-url")

	syncerCommand.Flags().StringVarP(&connString, "db-conn", "c", "",
		"[REQUIRED] PostgreSQL connection string (e.g. postgres://user:pass@host:5432/db)")
	syncerCommand.MarkFlagRequired("db-conn")
}

func setOptionalFlags() {

	syncerCommand.Flags().BoolVarP(&logging, "logging", "v", false,
		"enable logging")

	syncerCommand.Flags().Uint64Var(&pollInterval, "poll-interval", 2000,
		"interval in milliseconds between block polls")

	syncerCommand.Flags().BoolVar(&tipOnly, "tip-only", false,
		"apply poll interval (--poll-interval) only when syncer reaches the tip of the chain")

	// The txpool worker is scheduled for removal.

	// [SCHEDULED FOR REMOVAL]
	// syncerCommand.Flags().BoolVar(&syncTxPool, "sync-tx-pool", false,
	// 	"additionally synchronize (pending and queued) txs from the tx pool")

	// [SCHEDULED FOR REMOVAL]
	// syncerCommand.Flags().Uint64Var(&txPoolPollInterval, "tx-pool-poll-interval", 2000,
	// 	"interval in milliseconds between tx pool polls")

	syncerCommand.Flags().BoolVarP(&fullBlock, "full-block", "f", false,
		"when fetching a block, retrieve full tx data instead of only tx hashes")

	syncerCommand.Flags().Uint64VarP(&batchSize, "batch-size", "b", 1,
		"number of RPC calls per batch when fetching transaction data")

	syncerCommand.Flags().Uint64VarP(&txWorkers, "tx-workers", "w", 1,
		"(maximum) number of concurrent goroutines used to fetch transaction data")

	syncerCommand.Flags().BoolVar(&erc20Stats, "erc20-stats", false,
		"enable ERC-20 statistics aggregation for watchlisted tokens (mint, burn, transfer counts and volumes per UTC hour)")

	syncerCommand.Flags().Uint64Var(&erc20WatchlistCheckInterval, "erc20-wl-check-interval", 2000,
		"how often the ERC-20 watchlist is checked for changes, in milliseconds")

	syncerCommand.Flags().BoolVar(&erc20StartFromTip, "erc20-start-from-tip", false,
		"when a token is added to the watchlist, start processing from the current chain tip instead of its last processed block")

	syncerCommand.Flags().Uint64Var(&erc20ProcessInterval, "erc20-process-interval", 2000,
		"how often the syncer retries processing a block for ERC-20 events when it is not yet available, in milliseconds")

	syncerCommand.Flags().BoolVar(&eoaActivityStats, "eoa-activity-stats", false,
		"enable EOA activity tracking, recording the UTC hours in which each EOA address participated in a transaction")

	syncerCommand.Flags().Uint64Var(&eoaActivityProcessInterval, "eoa-activity-process-interval", 2000,
		"how often the syncer retries processing a block for EOA activity statistics when it is not yet available, in milliseconds")

	syncerCommand.Flags().Uint64Var(&circulationPollInterval, "circulation-poll-interval", 0,
		"interval in milliseconds between circulation polls")
}

func execute(cmd *cobra.Command, args []string) error {
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return fmt.Errorf("cannot open postgres db: %w", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("db ping error: %w", err)
	}

	sh, err := storage_handler.NewPgStorageHandler(db, fullBlock)
	if err != nil {
		return err
	}

	bwStartBlock, err := sh.GetLastBlockNumber()
	if err != nil {
		return err
	}

	if bwStartBlock == nil {
		bwStartBlock = new(uint64)
	} else {
		*bwStartBlock++
	}

	txwStartBlock, err := sh.GetTxWorkerLastBlockProcessed()
	if err != nil {
		return err
	}

	if txwStartBlock == nil {
		txwStartBlock = new(uint64)
	} else {
		*txwStartBlock++
	}

	opts := []syncer.SyncerOption{
		syncer.WithPollInterval(pollInterval),
		syncer.WithBatchSize(batchSize),
		syncer.WithMaxTxWorkers(txWorkers),
		syncer.WithBlockWorkerStartBlock(*bwStartBlock),
		syncer.WithTransactionkWorkerStartBlock(*txwStartBlock),
		syncer.WithCirculationPollInterval(circulationPollInterval),
	}

	if logging {
		opts = append(opts, syncer.WithLogger(helper.DefaultLogger{}))
	}

	if tipOnly {
		opts = append(opts, syncer.WithTipOnly())
	}

	if syncTxPool {
		opts = append(opts, syncer.WithTxPool(txPoolPollInterval))
	}

	if fullBlock {
		opts = append(opts, syncer.WithFullTransactions())
	}

	if erc20Stats {
		backend := erc20backend.NewPgErc20Backend(db)

		opts = append(opts, syncer.WithErc20Stats(backend),
			syncer.WithErc20ProcessInterval(erc20ProcessInterval),
			syncer.WithErc20WatchlistCheckInterval(erc20WatchlistCheckInterval))

		if erc20StartFromTip {
			opts = append(opts, syncer.WithErc20StartFromTip())
		}
	}

	if eoaActivityStats {
		backend := eoaactivitybackend.NewPgEoaActivityBackend(db)

		block, err := backend.GetLastProcessedBlock()
		if err != nil {
			return err
		}

		if block != nil {
			opts = append(opts, syncer.WithEoaActivityStartBlock(*block+1))
		}

		opts = append(opts, syncer.WithEoaActivityStats(backend),
			syncer.WithEoaActivityProcessInterval(eoaActivityProcessInterval))
	}

	if syn, err := syncer.NewSyncer(rpcUrl, sh, opts...); err == nil {
		syn.Start()
	} else {
		return err
	}

	return nil
}
