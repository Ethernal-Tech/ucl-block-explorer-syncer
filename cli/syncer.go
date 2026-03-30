package cli

import (
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/storage_handler"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/helper"
	"github.com/spf13/cobra"
)

var (
	rpcUrl             string
	connString         string
	logging            bool
	pollInterval       uint64
	tipOnly            bool
	syncTxPool         bool
	txPoolPollInterval uint64
	fullBlock          bool
	batchSize          uint64
	txWorkers          uint64
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

	syncerCommand.Flags().BoolVar(&syncTxPool, "sync-tx-pool", false,
		"additionally synchronize (pending and queued) txs from the tx pool")

	syncerCommand.Flags().Uint64Var(&txPoolPollInterval, "tx-pool-poll-interval", 2000,
		"interval in milliseconds between tx pool polls")

	syncerCommand.Flags().BoolVarP(&fullBlock, "full-block", "f", false,
		"when fetching a block, retrieve full tx data instead of only tx hashes")

	syncerCommand.Flags().Uint64VarP(&batchSize, "batch-size", "b", 1,
		"number of RPC calls per batch when fetching transaction data")

	syncerCommand.Flags().Uint64VarP(&txWorkers, "tx-workers", "w", 1,
		"(maximum) number of concurrent goroutines used to fetch transaction data")
}

func execute(cmd *cobra.Command, args []string) error {
	sh, err := storage_handler.NewPgStorageHandler(connString, fullBlock)
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

	if syn, err := syncer.NewSyncer(rpcUrl, sh, opts...); err == nil {
		syn.Start()
	} else {
		return err
	}

	return nil
}
