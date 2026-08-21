package cli

import (
	"database/sql"
	"fmt"
	"os"

	dataanchorbackend "github.com/Ethernal-Tech/ucl-block-explorer-syncer/data_anchor_backend"
	syncerdatabase "github.com/Ethernal-Tech/ucl-block-explorer-syncer/database"
	eoaactivitybackend "github.com/Ethernal-Tech/ucl-block-explorer-syncer/eoa_activity_backend"
	erc20backend "github.com/Ethernal-Tech/ucl-block-explorer-syncer/erc20_backend"
	esgaggregationbackend "github.com/Ethernal-Tech/ucl-block-explorer-syncer/esg_aggregation_backend"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/logging"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/storage_handler"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/utils"
	"github.com/spf13/cobra"
)

var (
	rpcUrl                      string
	connString                  string
	verboseLogging              bool
	logLevel                    string
	logFormat                   string
	pollInterval                uint64
	tipOnly                     bool
	syncTxPool                  bool
	txPoolPollInterval          uint64
	fullBlock                   bool
	batchSize                   uint64
	txWorkers                   uint64
	maxRetries                  int64
	retryInterval               uint64
	erc20Stats                  bool
	erc20WatchlistCheckInterval uint64
	erc20StartFromTip           bool
	erc20ProcessInterval        uint64
	dataAnchorStats             bool
	dataAnchorWatchlistPoll     uint64
	dataAnchorProcessInterval   uint64
	eoaActivityStats            bool
	eoaActivityProcessInterval  uint64
	circulationPollInterval     uint64

	esgAggregationStats        bool
	esgAggregationPollInterval uint64
	configPath                 string
	metricsAddr                string
	otelEndpoint               string
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
	_ = syncerCommand.MarkFlagRequired("rpc-url")

	syncerCommand.Flags().StringVarP(&connString, "db-conn", "c", "",
		"[REQUIRED] PostgreSQL connection string (e.g. postgres://user:pass@host:5432/db)")
	_ = syncerCommand.MarkFlagRequired("db-conn")
}

func setOptionalFlags() {
	syncerCommand.Flags().BoolVarP(&verboseLogging, "logging", "v", false,
		"enable lifecycle logging at info level; without it only warnings and errors are logged")

	syncerCommand.Flags().StringVar(&logLevel, "log-level", "",
		"log level: debug, info, warn or error; overrides --logging when set")

	syncerCommand.Flags().StringVar(&logFormat, "log-format", "json",
		"log output format: json (for log shipping) or text (for local development)")

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

	syncerCommand.Flags().Int64Var(&maxRetries, "max-retries", -1,
		"maximum number of attempts to fetch blockchain data before giving up and shutting down; -1 retries indefinitely")

	syncerCommand.Flags().Uint64Var(&retryInterval, "retry-interval", 5000,
		"interval in milliseconds between two consecutive retry attempts on failure")

	syncerCommand.Flags().BoolVar(&erc20Stats, "erc20-stats", false,
		"enable ERC-20 statistics aggregation for watchlisted tokens (mint, burn, transfer counts and volumes per UTC hour)")

	syncerCommand.Flags().Uint64Var(&erc20WatchlistCheckInterval, "erc20-wl-check-interval", 2000,
		"how often the ERC-20 watchlist is checked for changes, in milliseconds")

	syncerCommand.Flags().BoolVar(&erc20StartFromTip, "erc20-start-from-tip", false,
		"when a token is added to the watchlist, start processing from the chain tip instead of its last processed block")

	syncerCommand.Flags().Uint64Var(&erc20ProcessInterval, "erc20-process-interval", 2000,
		"how often the syncer retries processing a block for ERC-20 events when it is not yet available, in milliseconds")

	syncerCommand.Flags().BoolVar(&dataAnchorStats, "data-anchor-stats", false,
		"enable DailyCommitment factory discovery and commitment-count indexing")

	syncerCommand.Flags().Uint64Var(&dataAnchorWatchlistPoll, "data-anchor-watchlist-poll-interval", 2000,
		"how often the data-anchor factory watchlist is checked for changes, in milliseconds")

	syncerCommand.Flags().Uint64Var(&dataAnchorProcessInterval, "data-anchor-process-interval", 2000,
		"how often data-anchor workers retry blocks that are not indexed yet, in milliseconds")

	syncerCommand.Flags().BoolVar(&eoaActivityStats, "eoa-activity-stats", false,
		"enable EOA activity tracking, recording the UTC hours in which each EOA address participated in a transaction")

	syncerCommand.Flags().Uint64Var(&eoaActivityProcessInterval, "eoa-activity-process-interval", 2000,
		"how often the syncer retries processing a block for EOA activity when it is not yet available, in milliseconds")

	syncerCommand.Flags().Uint64Var(&circulationPollInterval, "circulation-poll-interval", 0,
		"interval in milliseconds between circulation polls")

	syncerCommand.Flags().BoolVar(&esgAggregationStats, "esg-aggregation-stats", false,
		"enable ESG aggregation tracking")

	syncerCommand.Flags().Uint64Var(&esgAggregationPollInterval, "esg-aggregation-poll-interval", 86400000,
		"how often the syncer polls for new ESG aggregation data, in milliseconds")

	syncerCommand.Flags().StringVar(&configPath, "config", "", "path to JSON config file")

	syncerCommand.Flags().StringVar(&metricsAddr, "metrics-addr", "0.0.0.0:2112",
		"TCP listen address for the Prometheus /metrics endpoint; pass an empty string to disable metrics")

	syncerCommand.Flags().StringVar(&otelEndpoint, "otel-endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		"OTLP trace collector endpoint (e.g. http://localhost:4317); empty disables span "+
			"export, but trace IDs still appear in logs")
}

func execute(cmd *cobra.Command, args []string) error {
	// Built first so slog.SetDefault is installed before any component logs anything,
	// including the database migrations below. Start-up failures themselves are returned
	// to cobra rather than logged here. --log-level wins when set; otherwise --logging
	// picks between info and warn, preserving the previous meaning of that flag.
	level := logLevel
	if level == "" {
		level = "warn"
		if verboseLogging {
			level = "info"
		}
	}

	logger := logging.Init(level, logFormat)

	config, err := utils.LoadConfig(configPath)
	if err != nil {
		return err
	}

	db, err := sql.Open("postgres", connString)
	if err != nil {
		return fmt.Errorf("cannot open postgres db: %w", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("db ping error: %w", err)
	}

	defer db.Close() //nolint:errcheck

	migrationLog := func(format string, args ...any) {
		logger.Info(fmt.Sprintf(format, args...), "component", "migrations")
	}

	if err := syncerdatabase.RunMigrations(db, migrationLog); err != nil {
		return fmt.Errorf("database migration failed: %w", err)
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
		syncer.WithLogger(logger),
		syncer.WithPollInterval(pollInterval),
		syncer.WithBatchSize(batchSize),
		syncer.WithMaxTxWorkers(txWorkers),
		syncer.WithRetry(maxRetries, retryInterval),
		syncer.WithBlockWorkerStartBlock(*bwStartBlock),
		syncer.WithTransactionkWorkerStartBlock(*txwStartBlock),
		syncer.WithMetrics(metricsAddr),
		syncer.WithTracing(otelEndpoint),
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

	if dataAnchorStats {
		backend := dataanchorbackend.NewPgDataAnchorBackend(db, logger)

		opts = append(opts,
			syncer.WithDataAnchorStats(backend),
			syncer.WithDataAnchorWatchlistCheckInterval(dataAnchorWatchlistPoll),
			syncer.WithDataAnchorProcessInterval(dataAnchorProcessInterval),
		)
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

	if esgAggregationStats {
		var esgCfg *utils.ESGAggregationBackendConfig
		if config != nil && config.Syncer != nil && config.Syncer.ESG != nil {
			esgCfg = config.Syncer.ESG
		} else {
			esgCfg = &utils.ESGAggregationBackendConfig{} // by default all filters are nil
		}

		backend := esgaggregationbackend.NewESGAggregationBackend(db, esgCfg)

		opts = append(opts, syncer.WithEsgAggregationStats(backend),
			syncer.WithESGAggregationPollInterval(esgAggregationPollInterval))
	}

	if syn, err := syncer.NewSyncer(rpcUrl, sh, opts...); err == nil {
		if err := syn.Start(); err != nil {
			return err
		}
	} else {
		return err
	}

	return nil
}
