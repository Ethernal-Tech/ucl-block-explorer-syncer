package syncer

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/metrics"
	abstractworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/abstract_worker"
	blockworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/block_worker"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/helper"
	prologworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/prolog_worker"
	txworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/tx_worker"
	txpoolworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/txpool_worker"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/tracing"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/versioning"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const emptyBlockSentinel = "notx"

// StorageHandler defines the interface that must be implemented by any storage backend used
// with the [Syncer]. The syncer processes blocks and transactions from an EVM-based blockchain
// sequentially, and relies on this interface (that is, the underlying storage) to persist the
// processed (indexed) data. Implementations of this interface can use any storage mechanism,
// such as a relational database, a key-value store, or a file-based system, as long as they
// correctly implement the methods described below.
type StorageHandler interface {
	// InsertBlock is invoked every time a new block is fetched and deserialized. Depending on
	// the withTxs flag (configurable through [WithFullTransactions] option function), the block
	// will either contain complete transaction objects or only their hashes. In the latter case,
	// only the Hash field of each transaction is guaranteed to be correct, all other fields may
	// contain any value, even correct ones, and should not be relied upon. When withTxs is set,
	// only receipt-related fields of the transaction (e.g. Status) may contain any value, even
	// correct ones, and should not be relied upon. In both previously mentioned cases BlockHash,
	// BlockNumber and BlockTimestamp of each transaction are always correctly populated. If the
	// method returns an error, the syncer will immediately shut down gracefully.
	//
	// Note: the block and its contents must be treated as read-only. Mutating any fields results
	// in undefined behavior.
	InsertBlock(*types.Block) error

	// GetBlock returns the block with the given number. The only fields that must be correctly
	// set are Hash, Number and Timestamp in the block header, and Hash, BlockHash, BlockNumber
	// and BlockTimestamp in each transaction. All other fields can be arbitrary. Once the block
	// is returned, any modification by the implementor is considered undefined behavior. If the
	// method returns an error, the syncer will shut down gracefully.
	GetBlock(number uint64) (*types.Block, error)

	// InsertTransactions is invoked once per block, after all transactions in the block have
	// been fetched and deserialized. Depending on the configuration (withTx flag and the return
	// value of [StorageHandler.ShouldFetchFullTransaction] method), fetching may include only
	// the receipt or full transaction data. In the former case, only receipt-related fields
	// (e.g. Status) together with Hash, BlockHash, BlockNumber and BlockTimestamp are guaranteed
	// to be correct, all other fields may contain any value, even correct ones, and should not
	// be relied upon. When full transaction data is fetched, all fields are populated correctly.
	// It is guaranteed that [StorageHandler.InsertBlock] will be called for the block these
	// transactions belong to before this method is invoked. If the block does not contain any
	// transaction, this method is still invoked with a single sentinel transaction whose Hash
	// is set to "notx" - an invalid hash, ensuring no collision with real transaction hashes is
	// possible. BlockHash, BlockNumber and BlockTimestamp are also set correctly for the sentinel
	// transaction. If the method returns an error, the syncer will immediately shut down gracefully.
	InsertTransactions([]*types.Transaction) error

	// ShouldFetchFullTransaction returns whether full transaction data should be fetched for
	// the given transaction hash. If false, only the receipt will be fetched. This can be used,
	// for example, to selectively fetch full data based on an in-memory cache or other storage
	// lookup. The argument is the transaction hash string, including the leading hexadecimal
	// 0x prefix.
	ShouldFetchFullTransaction(hash string) bool

	// [SCHEDULED FOR REMOVAL]
	// InsertPoolTransactions is invoked every time the current state of the transaction pool is
	// fetched, i.e. every time (pending and queued) transactions are retrieved and deserialized
	// from it. If the method returns an error, the syncer will terminate immediately.
	InsertPoolTransactions(pending, queued []*types.Transaction) error
}

// Erc20Backend defines the interface that must be implemented by any backend used for ERC-20
// statistics processing. It is required when the syncer is configured with the [WithERC20Stats]
// option. The syncer is capable of tracking mint, burn, and transfer statistics - aggregated
// into UTC-hour buckets - for both private and non-private ERC-20 tokens. To do so correctly,
// it requires a backend that satisfies this interface. For each tracked token, the syncer
// creates a separate, independent internal instance responsible for monitoring that token's
// activity. Since multiple such instances may run concurrently, all methods of this interface
// must be safe for concurrent use.
type Erc20Backend interface {
	// GetWatchlist returns the list of ERC-20 tokens the syncer should track. Only tokens with
	// the Enabled field set to true are actively tracked - for each private/normal token, the
	// syncer maintains a separate internal instance responsible for collecting its statistics.
	// On every call, the syncer compares the returned list against its current tracking state
	// and starts or stops instances accordingly:
	//   - If a token was being tracked (i.e. previously returned with Enabled set to true) but
	//     now is not returned or returned with Enabled set to false, its tracking instance will
	//     be shut down.
	//   - If a token is returned with Enabled set to true and is not yet being tracked (i.e. was
	//     not previously returned, or was previously returned with Enabled set to false), a new
	//     tracking instance will be created, starting from the block defined by NextBlock.
	//     Starting block can be overridden via [WithErc20StartFromTip].
	// The CreatedAt and UpdatedAt fields are not used and may contain any value. Once returned,
	// the list and the tokens within it must not be modified by the implementer (backend). If
	// the method returns an error, the syncer will immediately shut down gracefully.
	GetWatchlist() ([]*types.ERC20Token, error)

	// GetTip returns the number of the last fully processed block - meaning all transactions
	// and their receipts are available for that block. If the method returns an error, the syncer
	// will immediately shut down gracefully.
	GetTip() (uint64, error)

	// GetBlock returns the block with the given number. Only the header fields are required to
	// be correctly populated - transaction data is not used here because the syncer relies on
	// [Erc20Backend.GetLogs] for log retrieval. If [Erc20Backend.GetLogs] were not available,
	// full transaction data and receipts would be required to extract logs. Once returned, the
	// block must not be modified by the implementer (backend). If the method returns an error,
	// the syncer will immediately shut down gracefully.
	GetBlock(number uint64) (*types.Block, error)

	// GetLogs returns all logs emitted by the given token address in the given block that contain
	// at least one of the provided topics. This method exists to leverage database indexing and
	// selection for fast filtering and retrieval. Without this method, the syncer would have to
	// fetch all logs within a block via [Erc20Backend.GetBlock] and perform the filtering itself.
	// The returned logs are used directly for ERC-20 stats aggregation. Once returned, the list
	// and the logs within it must not be modified by the implementer (backend). If the method
	// returns an error, the syncer will immediately shut down gracefully.
	GetLogs(blockNum uint64, tokenAddr string, topics []string) ([]types.ReceiptLog, error)

	// ProcessHourlyStat is invoked once per processed block for each tracked token. It should
	// process the aggregated Transfer event counts and volumes for the UTC hour derived from
	// the block timestamp. counts and volumes are keyed by transfer class: "transfer", "mint",
	// or "burn".
	ProcessHourlyStat(blockNum uint64,
		token *types.ERC20Token,
		hour time.Time,
		counts map[string]uint64,
		volumes map[string]*big.Int) error
}

// DataAnchorBackend provides watchlist, indexed-log, and atomic persistence
// operations for DailyCommitment factory workers.
type DataAnchorBackend interface {
	GetWatchlist() ([]*types.DataAnchorFactory, error)
	GetTip() (*uint64, error)
	GetLogs(blockNumber uint64) ([]types.ReceiptLog, error)
	ProcessBlock(blockNumber uint64, factory *types.DataAnchorFactory, logs []types.ReceiptLog) error
}

// ESGAggregationBackend defines the interface that must be implemented by any backend used
// for ESG calculation and aggregation. It is required when the syncer is configured with the
// [WithESGAggregationStats].
type ESGAggregationBackend interface {
	// Process executes the ESG aggregation logic.
	Process(context.Context, func(string, ...any)) (done bool, wait bool, err error)
}

// EoaActivityBackend defines the interface that must be implemented by any backend used for EOA
// activity tracking. It is required when the syncer is configured with the [WithEoaActivityStats]
// option. The syncer sequentially retrieves transaction participants (sender and receiver) for
// each block via [EoaActivityBackend.GetBlockParticipants], filters out non-EOA addresses, and
// forwards the resulting list of EOA addresses to [EoaActivityBackend.RecordEOAActivity] for
// further processing. The backend is solely responsible for defining what statistics are derived
// and persisted from the provided data.
type EoaActivityBackend interface {
	// GetBlockParticipants returns the list of transaction participants (from and to addresses)
	// for the given block. The method must only return data for blocks that have been fully
	// processed - meaning all transactions and their receipts are available. If the requested
	// block has not yet been fully processed, nil must be returned without an error, signaling
	// the syncer to wait before retrying. Once returned, the list and the participants within
	// it must not be modified by the implementer (backend). If the method returns an error,
	// the syncer will immediately shut down gracefully.
	GetBlockParticipants(blockNum uint64) ([]*types.BlockParticipant, error)

	// FilterKnownEOAs returns the subset of the provided addresses that are already known, that
	// is, already being tracked by the backend. Once returned, the list must not be modified
	// by the implementer (backend). If the method returns an error, the syncer will immediately
	// shut down gracefully.
	FilterKnownEOAs(addresses []string) ([]string, error)

	// RecordEOAActivity is invoked once per processed block. The provided addresses represent
	// the EOA addresses that were active in the given block. The backend is responsible for
	// deriving and persisting any statistics from this data. If the method returns an error,
	// the syncer will immediately shut down gracefully.
	RecordEOAActivity(blockNum uint64, addresses []string) error
}

// Syncer indexes an EVM-based blockchain by fetching and processing blocks and transactions via
// block and transaction workers, persisting the data to a storage backend. Additional workers
// can be enabled through constructor option functions - for example, [WithErc20Stats] enables
// tracking of ERC-20 token statistics. It supports different indexing strategies configurable
// through a set of constructor option functions (for example, [WithPollInterval]), that can be
// passed to [NewSyncer].
type Syncer struct {
	// rpcURL is the Ethereum RPC endpoint URL used to establish a connection to the blockchain
	// node. The underlying RPC client is created from this URL.
	rpcURL string

	// storage is responsible for persisting indexed data. For details, see the [StorageHandler]
	// interface documentation.
	storage StorageHandler

	// Optional fields (settable through [NewSyncer] constructor function):

	// logger records state changes and actions during the syncer's lifecycle. Defaults to
	// the process-wide slog logger; never silent.
	logger *slog.Logger

	// startBlockBW is the block number from which the block worker begins processing. By default, 0.
	startBlockBW uint64

	// startBlockTW is the block number from which the transaction workers begin processing. By
	// default, 0.
	startBlockTW uint64

	// pollInterval specifies how often the syncer attempts to fetch and process new blocks. By
	// default, 2000 milliseconds.
	pollInterval uint64

	// tipOnly controls when the pollInterval is applied. When true, the syncer runs without any
	// delay until it reaches the tip of the chain, after which pollInterval takes effect. When
	// false, pollInterval is applied between every iteration regardless of chain position. By
	// default, false.
	tipOnly bool

	// [SCHEDULED FOR REMOVAL]
	// syncTxPool specifies whether the syncer should also fetch (pending) transactions from the
	// transaction pool. By default, false.
	syncTxPool bool

	// [SCHEDULED FOR REMOVAL]
	// txPoolPollInterval specifies how often the syncer attempts to fetch pending transactions
	// from the transaction pool. By default, 2000 milliseconds.
	txPoolPollInterval uint64

	// withTxs specifies whether blocks should be fetched with full transaction objects or only
	// transaction hashes. When true, each block will contain complete transaction objects. When
	// false, only transaction hashes are included. Please read [StorageHandler.InsertBlock] and
	// [StorageHandler.InsertTransactions] documentation for more details. By default, false.
	withTxs bool

	// maxRetries is the maximum number of attempts to fetch (and process) a blockchain data
	// (blocks, transactions, etc.) before giving up and shutting down the worker(s). -1 denotes
	// indefinitely. By default, the first failure is treated as fatal.
	maxRetries int64

	// retryInterval specifies how long the syncer waits between two consecutive retry attempts,
	// in milliseconds. By default, 2000 milliseconds.
	retryInterval uint64

	// batchSize is the number of RPC calls grouped into a single batch request by tx workers.
	// By default, 1, meaning each RPC call is sent individually without batching.
	batchSize uint64

	// maxTxWorkers is the maximum number of transaction workers that can be active at a time.
	// Not all workers may be active for every block - for example, if the number of transactions
	// in a block is smaller than the batch size, only one worker will be assigned a job. By
	// default, 1.
	maxTxWorkers uint64

	// erc20Backend is the backend required for processing ERC-20 events. If nil, processing is
	// disabled. For details, see the [Erc20Backend] interface documentation.
	erc20Backend Erc20Backend

	// erc20WatchlistCheckInterval specifies how often the syncer checks the ERC-20 watchlist
	// for changes, in milliseconds. By default, 2000 milliseconds.
	erc20WatchlistCheckInterval uint64

	// erc20StartFromTip controls the block from which the syncer begins processing ERC-20 events
	// for a newly enabled token. When true, the syncer starts from the current tip of the chain
	// (retrieved via [Erc20Backend.GetTip]), skipping all historical blocks. When false, it
	// starts from the block defined by the token's NextBlock field in the watchlist. By default,
	// false.
	erc20StartFromTip bool

	// erc20ProcessInterval specifies how long the syncer waits before retrying to process a block
	// for ERC-20 events when it is not yet available. By default, 2000 milliseconds.
	erc20ProcessInterval uint64

	dataAnchorBackend                DataAnchorBackend
	dataAnchorWatchlistCheckInterval uint64
	dataAnchorProcessInterval        uint64

	// eoaActivityBackend is the backend required for processing EOA activities. If nil, processing
	// is disabled. For details, see the [EoaActivityBackend] interface documentation.
	eoaActivityBackend EoaActivityBackend

	// eoaActivityProcessInterval specifies how long the syncer waits before retrying to process a
	// block for EOA activity statistics when it is not yet available. By default, 2000 milliseconds.
	eoaActivityProcessInterval uint64

	// eoaActivityStartBlock is the block number from which the EOA activity worker begin processing.
	// By default, 0.
	eoaActivityStartBlock uint64

	// esgAggregationBackend is the backend required for processing ESG aggregation. If nil, processing
	// is disabled. For details, see the [ESGAggregationBackend] interface documentation.
	esgAggregationBackend ESGAggregationBackend

	// esgAggregationPollInterval specifies how often the syncer attempts to execute ESG aggregation
	// logic. By default, once per day.
	esgAggregationPollInterval uint64

	// metricsEnabled gates the /metrics endpoint and the sampler goroutine (instruments are always
	// recorded). Enabled via [WithMetrics] with a non-empty address. By default, false.
	metricsEnabled bool

	// metricsAddr is the TCP listen address for the /metrics endpoint. Only used when metricsEnabled.
	metricsAddr string

	// tracingEndpoint is the OTLP trace collector endpoint, set via [WithTracing]. Empty means
	// spans are created but not exported; tracing itself is always on, since trace IDs are
	// what join the syncer's logs to its traces.
	tracingEndpoint string

	// tracerShutdown flushes and stops the tracer provider on shutdown. nil when init failed.
	tracerShutdown func(context.Context) error

	// Internal fields used by the syncer:

	// m, s, and l form an internal block queue used to pass blocks from the block worker, via
	// the dispatcher, to the transaction workers. `m` protects access to `l` (where blocks are
	// stored), and `s` is a channel used to notify the dispatcher when a new block is added -
	// it must be buffered channel (at least capacity of 1) to avoid missed signals in some edge
	// cases.

	m sync.Mutex
	s chan struct{}
	l *list.List

	// bwHandle holds the handle for the block worker managed by the syncer.
	bwHandle *blockWorkerHandle

	// txwHandles holds the handles for all transaction workers managed by the syncer.
	txwHandles []*txWorkerHandle

	// erc20wHandles holds the handles for all erc20 workers managed by the syncer.
	erc20wHandles map[string]*erc20WorkerHandle

	dataAnchorwHandles map[string]*dataAnchorWorkerHandle

	// eoaawHandle holds the handle for the EOA activity worker managed by the syncer.
	eoaawHandle *eoaActivityWorkerHandle

	// txpwHandle holds the handle for the transaction pool worker managed by the syncer.
	txpwHandle *txPoolWorkerHandle

	// esgAggregationWorkerHandle holds the handle for the ESG aggregation worker managed by the syncer.
	esgAggregationWorkerHandle *esgAggregationWorkerHandle

	// shutDownCh is closed to signal all workers (that is, their controller goroutines) to shut
	// down gracefully.
	shutDownCh chan struct{}
	once       sync.Once

	// lastIndexedBlock holds the number of the most recently fully-indexed block (block and its
	// transactions persisted). It is written by the transaction worker controller goroutine and
	// read by the metrics sampler goroutine, hence atomic.
	lastIndexedBlock atomic.Uint64

	// txJobsInFlight holds the number of tx-worker jobs dispatched but not yet completed. Written
	// by the tx worker controller goroutine and read by the metrics sampler goroutine, hence atomic.
	txJobsInFlight atomic.Int64
}

// NewSyncer constructs a new [Syncer] instance. rpcURL must be a valid RPC endpoint URL used
// to establish a connection to the EVM-based node. storage is the persistence layer used for
// indexed data, see [StorageHandler] for details. It cannot be nil.
//
// The following optional configurations are available (see their documentation for details):
//  1. WithLogger (default: the process-wide slog logger)
//  2. WithBlockWorkerStartBlock (default: 0)
//  3. WithTransactionWorkerStartBlock (default: 0)
//  4. WithPollInterval (default: 2000 milliseconds)
//  5. WithTipOnly (default: false)
//  6. WithTxPool (default: disabled) [SCHEDULED FOR REMOVAL]
//  7. WithFullTransactions (default: false)
//  8. WithRetry (default: first failure is treated as fatal)
//  9. WithBatchSize (default: 1)
//  10. WithMaxTxWorkers (default: 1)
//  11. WithErc20Stats (default: disabled)
//  12. WithErc20WatchlistCheckInterval (default: 2000 milliseconds)
//  13. WithErc20StartFromTip (default: false)
//  14. WithErc20ProcessInterval (default: 2000 milliseconds)
//  15. WithEoaActivityStats (default: disabled)
//  16. WithEoaActivityProcessInterval (default: 2000 milliseconds)
//  17. WithEoaActivityStartBlock (default: 0)
//  18. WithESGAggregationStats (default: disabled)
//  19. WithESGAggregationProcessInterval (default: once per day at midnight UTC)
func NewSyncer(
	rpcURL string,
	storage StorageHandler,
	opts ...SyncerOption) (*Syncer, error) {
	switch {
	case rpcURL == "":
		return nil, fmt.Errorf("rpcURL cannot be empty string")
	case storage == nil:
		return nil, fmt.Errorf("storage (handler) cannot be nil")
	}

	syncer := &Syncer{
		// Never nil: components fall back to the process default so a syncer started
		// without WithLogger still reports what it is doing.
		logger: slog.Default(),

		rpcURL:                           rpcURL,
		storage:                          storage,
		maxRetries:                       1,
		retryInterval:                    2000,
		batchSize:                        1,
		maxTxWorkers:                     1,
		pollInterval:                     2000,
		txPoolPollInterval:               2000,
		erc20WatchlistCheckInterval:      2000,
		erc20ProcessInterval:             2000,
		dataAnchorWatchlistCheckInterval: 2000,
		dataAnchorProcessInterval:        2000,
		eoaActivityProcessInterval:       2000,
		esgAggregationPollInterval:       uint64((24 * time.Hour).Milliseconds()),
	}

	for _, o := range opts {
		if err := o(syncer); err != nil {
			return nil, err
		}
	}

	syncer.s = make(chan struct{}, 1)
	syncer.l = list.New()
	syncer.shutDownCh = make(chan struct{})

	// Seed last-indexed with the block before startBlockTW (the first block to be
	// indexed) so the indexing-lag gauge is meaningful from the first sample.
	if syncer.startBlockTW > 0 {
		syncer.lastIndexedBlock.Store(syncer.startBlockTW - 1)
	}

	// Block worker handle construction.
	{
		client, err := syncer.dialRPC(rpcURL)
		if err != nil {
			return nil, fmt.Errorf("cannot establish RPC connection for block worker: %w", err)
		}

		bwh, err := syncer.createBlockWorkerHandle(
			0,
			client,
			make(chan struct{}, 1),
			make(chan struct{}, 1),
			make(chan error, 1),
			syncer.startBlockBW,
		)
		if err != nil {
			return nil, err
		}

		syncer.bwHandle = bwh
	}

	// Transaction worker handles construction.
	{
		doneCh := make(chan uint64, syncer.maxTxWorkers)
		errCh := make(chan struct {
			Err error
			Id  uint64
		}, syncer.maxTxWorkers)

		client, err := syncer.dialRPC(rpcURL)
		if err != nil {
			return nil, fmt.Errorf("cannot establish RPC connection for tx worker(s): %w", err)
		}

		for i := range syncer.maxTxWorkers {
			txwh, err := syncer.createTxWorkerHandle(
				i+1,
				client,
				make(chan txworker.TxJob, 1),
				doneCh,
				errCh,
				syncer.startBlockTW,
			)
			if err != nil {
				return nil, err
			}

			syncer.txwHandles = append(syncer.txwHandles, txwh)
		}
	}

	// ERC20 worker handles construction.
	if syncer.erc20Backend != nil {
		syncer.erc20wHandles = map[string]*erc20WorkerHandle{}

		// Unlike the transaction workers, the number of tracked ERC-20 tokens (and therefore
		// the number of ERC-20 workers) may change between the time the syncer is created and
		// the time it is started. Therefore, the handles for ERC-20 workers are not initialized
		// here but deferred until the syncer is started.
	}

	if syncer.dataAnchorBackend != nil {
		syncer.dataAnchorwHandles = map[string]*dataAnchorWorkerHandle{}
	}

	// EOA activity worker handle construction.
	if syncer.eoaActivityBackend != nil {
		eoaawh, err := syncer.createEoaActivityWorkerHandle()
		if err != nil {
			return nil, err
		}

		syncer.eoaawHandle = eoaawh
	}

	// ESG aggregation worker handle construction.
	if syncer.esgAggregationBackend != nil {
		esgawh, err := syncer.createESGAggregationWorkerHandle(context.Background())
		if err != nil {
			return nil, err
		}

		syncer.esgAggregationWorkerHandle = esgawh
	}

	// [SCHEDULED FOR REMOVAL]
	// Transaction pool worker handle construction.
	if syncer.syncTxPool {
		client, err := syncer.dialRPC(rpcURL)
		if err != nil {
			return nil, fmt.Errorf("cannot establish RPC connection for tx pool worker: %w", err)
		}

		txpwh, err := syncer.createTxPoolWorkerHandle(
			0,
			client,
			make(chan struct{}, 1),
			make(chan struct{}, 1),
			make(chan error, 1),
		)
		if err != nil {
			return nil, err
		}

		syncer.txpwHandle = txpwh
	}

	return syncer, nil
}

// Start starts the syncer by launching the block, transaction, and any additionally configured
// workers (such as the ERC-20 stats workers). It returns an error if the syncer fails to start.
// Workers that depend on runtime state, such as the ERC-20 stats workers - whose number depends
// on the token watchlist at the time of startup, are initialized here rather than in [NewSyncer].
// Once running, the syncer operates until a fatal error occurs or it is stopped externally. For
// details on how the syncer orchestrates and manages its workers, see the detailed comments
// within this function.
func (s *Syncer) Start() error {
	defer s.shutDown()

	if s.storage == nil {
		return fmt.Errorf(
			"method must be invoked on an instance initialized through [NewTxWorker]")
	}

	// Set up tracing before the workers start making RPC calls, so their outbound
	// requests carry the W3C traceparent from the very first block. This runs even
	// with no collector configured: the provider then exports nothing, but spans
	// still carry valid trace IDs for log correlation and outbound propagation.
	shutdown, err := tracing.Init(context.Background(), s.tracingEndpoint, versioning.Version)
	if err != nil {
		s.logWarn("tracing init failed: %v", err)
	} else {
		s.tracerShutdown = shutdown

		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := shutdown(ctx); err != nil {
				s.logWarn("tracer shutdown error: %v", err)
			}
		}()
	}

	if err := s.bwHandle.bw.Start(); err != nil {
		return fmt.Errorf("cannot start block worker: %w", err)
	}

	for _, handle := range s.txwHandles {
		if err := handle.txw.Start(); err != nil {
			return fmt.Errorf("cannot start tx worker: %w", err)
		}
	}

	if s.eoaawHandle != nil {
		if err := s.eoaawHandle.eoaaw.Start(); err != nil {
			return fmt.Errorf("cannot start EOA activity worker: %w", err)
		}
	}

	if s.esgAggregationWorkerHandle != nil {
		if err := s.esgAggregationWorkerHandle.worker.Start(); err != nil {
			return fmt.Errorf("cannot start ESG aggregation worker: %w", err)
		}
	}

	if s.syncTxPool {
		if err := s.txpwHandle.txpw.Start(); err != nil {
			return fmt.Errorf("cannot start block worker: %w", err)
		}
	}

	s.log("started")

	wg := sync.WaitGroup{}

	wg.Add(2)

	// We don't care about txpool worker since it is scheduled for removal.

	if s.erc20Backend != nil {
		wg.Add(1)
	}

	if s.dataAnchorBackend != nil {
		wg.Add(1)
	}

	if s.eoaActivityBackend != nil {
		wg.Add(1)
	}

	if s.esgAggregationBackend != nil {
		wg.Add(1)
	}

	// Block worker controller goroutine - responsible for managing the block worker lifecycle.
	// It has two tasks/responsibilities:
	//
	// 1. Listens for fatal errors from the block worker. Since a value sent to errCh indicates
	//    the block worker has already shut down, it logs the error and signals the other worker
	//    worker controllers to shut down as well.
	//
	// 2. Listens for a shutdown signal from the other worker controllers. Upon receiving it,
	//    it signals the block worker to stop by closing ctrlCh, and waits for it to shut down
	//    down gracefully via doneCh.
	go func() {
		defer wg.Done()

		select {
		case err := <-s.bwHandle.errCh:
			s.logErr("block worker encountered a fatal error: %s", err.Error())

			s.shutDownHandles()
		case <-s.shutDownCh:
			close(s.bwHandle.ctrlCh)

			select {
			case err := <-s.bwHandle.errCh:
				s.logErr("block worker encountered a fatal error: %s", err.Error())
			case <-s.bwHandle.doneCh:
			}
		}
	}()

	// Transaction worker controller goroutine - responsible for managing the transaction workers'
	// lifecycle. It has three tasks/responsibilities:
	//
	// 1. Dispatches jobs to transaction workers for each block. If the transaction workers are
	//    behind the block worker's start block (e.g. after a restart where the block worker was
	//    ahead), blocks are read directly from storage until the gap is closed. Once caught up,
	//    blocks are consumed from the internal block cache as the block worker produces them.
	//
	// 2. Waits for all active workers to complete their jobs. Once all workers have finished,
	//    writes the block to the storage. If any worker encounters a fatal error, it initiates
	//    a graceful shutdown of all other transaction workers and signals the other worker
	// 	  controllers to shut down as well.
	//
	// 3. Listens for a shutdown signal from the other worker controllers. Upon receiving it,
	//    initiates a graceful shutdown of all transaction workers.
	go func() {
		defer wg.Done()

		// shutDownFn signals the transaction workers (by closing their job channels) and the
		// other worker controllers (by closing shutDown channel) to shut down, and waits for
		// all active transaction workers to shut down gracefully via doneCh. numOfAlreadyDown
		// indicates how many transaction workers have already shut down (due to error) and
		// should not be waited on.
		shutDownFn := func(numOfAlreadyDown int) {
			s.shutDownHandles()

			for _, t := range s.txwHandles {
				close(t.jobCh)
			}

			for range len(s.txwHandles) - numOfAlreadyDown {
				<-s.txwHandles[0].doneCh
			}
		}

		currentBlock := s.txwHandles[0].startBlock

	break_for:
		for {
			s.log("processing block %v", currentBlock)

			var (
				block *types.Block
				err   error
			)

			if currentBlock >= s.bwHandle.startBlock {
				block = s.getBlock()
			} else {
				block, err = s.storage.GetBlock(currentBlock)
				if err != nil {
					s.logErr("cannot get block %v: %s", currentBlock, err.Error())

					shutDownFn(0)

					break
				}
			}

			select {
			case <-s.shutDownCh:
				shutDownFn(0)

				break break_for
			default:
			}

			// Captured before the empty-block sentinel may be appended below, so
			// the txs-processed counter reflects only real transactions.
			realTxCount := len(block.Transactions)

			// The block span parents every worker span for this block. It is started
			// here rather than at the top of the iteration so it covers exactly the
			// fan-out and the wait, which is the work the tx workers actually do.
			blockCtx, blockSpan := tracing.Tracer().Start(context.Background(), "block.index",
				trace.WithAttributes(
					attribute.Int64("block.number", int64(currentBlock)),
					attribute.Int("block.tx_count", realTxCount),
				))

			jobs := helper.CreateJobs(uint64(len(block.Transactions)), uint64(len(s.txwHandles)))

			s.logCtx(blockCtx, "%v jobs created", len(jobs))

			for i, job := range jobs {
				job.Block = block

				// Carries the block's trace context across the channel into the worker
				// goroutine, which has no other way to reach it.
				job.Ctx = blockCtx

				s.txwHandles[i].jobCh <- job

				s.txJobsInFlight.Add(1)

				s.logCtx(blockCtx, "job [%v-%v] dispatched", job.From, job.To)
			}

			l := len(jobs)
			errOcured := 0

			for l != 0 {
				select {
				case id := <-s.txwHandles[0].doneCh:
					s.log("tx worker %v finished", id)

					s.txJobsInFlight.Add(-1)

					l--
				case err := <-s.txwHandles[0].errCh:
					s.logErr("transaction worker %v encountered a fatal error: %s", err.Id, err.Err.Error())

					s.txJobsInFlight.Add(-1)

					errOcured++
					l--
				}
			}

			blockSpan.End()

			if errOcured != 0 {
				shutDownFn(errOcured)

				break
			}

			// If the block does not contain any transactions, a sentinel transaction is used
			// with Hash set to [emptyBlockSentinel] (which is an invalid hash), so the storage
			// handler can detect that the transaction processing phase for this block has
			// completed.
			if len(block.Transactions) == 0 {
				block.Transactions = append(block.Transactions, &types.Transaction{
					Hash:           emptyBlockSentinel,
					BlockHash:      &block.Hash,
					BlockNumber:    &block.Number,
					BlockTimestamp: &block.Timestamp,
				})
			}

			if err := s.storage.InsertTransactions(block.Transactions); err != nil {
				s.logErr("cannot insert transactions: %v", err.Error())

				shutDownFn(errOcured)

				break
			}

			s.log("block %v processed", currentBlock)

			// lastIndexedBlock feeds the indexing-lag gauge sampled by the metrics goroutine.
			metrics.BlocksProcessed.Inc()
			metrics.TxsProcessed.Add(float64(realTxCount))
			s.lastIndexedBlock.Store(currentBlock)

			currentBlock++
		}
	}()

	// ERC-20 worker controller goroutine - responsible for managing the lifecycle of ERC-20
	// workers. ERC-20 worker handles are constructed here (deferred from the [Syncer.Start]).
	// It has three responsibilities:
	//
	//  1. Periodically checks (as defined by [Syncer.erc20WatchlistCheckInterval]) the state
	//     of the token watchlist and starts or stops workers accordingly.
	//
	//  2. Monitors all active ERC-20 workers for fatal errors. Upon detecting one, initiates
	//     a graceful shutdown of all remaining ERC-20 workers and signals the other worker
	//     controllers to shut down as well by closing [Syncer.shutDownCh].
	//
	//  3. Listens for a shutdown signal (closing of [Syncer.shutDownCh]) from the other worker
	//     controllers. Upon receiving it, initiates a graceful shutdown of all ERC-20 workers.
	if s.erc20Backend != nil {
		go func() {
			defer wg.Done()

			// shutDownFn initiates a graceful shutdown of all active ERC-20 workers by closing
			// their control channels and waiting for each to shut down. It also signals the
			// other worker controllers to shut down by closing [Syncer.shutDownCh].
			shutDownFn := func() {
				s.shutDownHandles()

				for _, handle := range s.erc20wHandles {
					close(handle.ctrlCh)

					select {
					// It can happen that the ERC-20 worker encountered a fatal error in the
					// meantime and has already shut down, in which case we would never receive
					// a signal on the done channel.
					case err := <-handle.errCh:
						s.logErr("ERC-20 worker for token %s encountered a fatal error: %s",
							*handle.token.Symbol,
							err.Err.Error())
					case <-handle.doneCh:
					}
				}
			}

			for {
				select {
				case <-s.shutDownCh:
					shutDownFn()

					return
				default:
				}

				s.log("fetching token watchlist")

				tokens, err := s.erc20Backend.GetWatchlist()
				if err != nil {
					s.logWarn("failed to fetch the token watchlist: %s", err.Error())

					shutDownFn()

					return
				}

				s.log("token watchlist: %s", strings.Join(func() []string {
					symbols := make([]string, 0, len(tokens))

					for _, t := range tokens {
						if !t.Enabled {
							continue
						}

						symbol := "/"

						if t.Symbol != nil {
							symbol = *t.Symbol
						}

						symbols = append(symbols, fmt.Sprintf("%s (%s)", symbol, t.Address))
					}

					if len(symbols) == 0 {
						return []string{"empty"}
					}

					return symbols
				}(), ", "))

				// Build a set of active token addresses from the current watchlist.
				activeTokens := make(map[string]*types.ERC20Token, len(tokens))
				for _, token := range tokens {
					if token.Enabled {
						activeTokens[token.Address] = token
					}
				}

				// Stop workers for tokens that are no longer active, that is, that are removed
				// from the watchlist or their Enabled field is set to false.
				for address, handle := range s.erc20wHandles {
					if _, ok := activeTokens[address]; ok {
						continue
					}

					s.log("token %s disabled or removed from watchlist, stopping worker",
						*handle.token.Symbol)

					delete(s.erc20wHandles, address)

					close(handle.ctrlCh)

					select {
					// It can happen that the ERC-20 worker encountered a fatal error in the
					// meantime and has already shut down, in which case we would never receive
					// a signal on the done channel.
					case err := <-handle.errCh:
						s.logErr("ERC-20 worker for token %s encountered a fatal error: %s",
							*handle.token.Symbol,
							err.Err.Error())

						shutDownFn()

						return
					case <-handle.doneCh:
					}

					s.log("ERC-20 worker for token %s successfully shut down", *handle.token.Symbol)
				}

				// Start workers for tokens that are newly added or re-enabled in the watchlist.
				for _, token := range activeTokens {
					if _, ok := s.erc20wHandles[token.Address]; ok {
						continue
					}

					if token.IsPrivate {
						s.log("new private ERC-20 token %s (address: %s) in watchlist",
							*token.Symbol,
							token.Address)
					} else {
						s.log("new ERC-20 token %s (address: %s) in watchlist",
							*token.Symbol,
							token.Address)
					}

					if s.erc20StartFromTip {
						tip, err := s.erc20Backend.GetTip()
						if err != nil {
							s.logWarn("failed to fetch the tip of the chain: %s", err.Error())

							shutDownFn()

							return
						}

						token.NextBlock = tip
					}

					handle, err := s.createErc20WorkerHandle(token,
						make(chan struct{}, 1),
						make(chan string, 1),
						make(chan struct {
							Err error
							Id  string
						}, 1))
					if err != nil {
						s.logErr("failed to create ERC-20 worker for token %s: %s",
							*token.Symbol,
							err.Error())

						shutDownFn()

						return
					}

					if err := handle.erc20w.Start(); err != nil {
						s.logErr("failed to start ERC-20 worker for token %s: %s",
							*token.Symbol,
							err.Error())

						shutDownFn()

						return
					}

					s.erc20wHandles[token.Address] = handle

					s.log("ERC-20 worker for token %s successfully created and started",
						*token.Symbol)
				}

				// Since the number of ERC-20 workers is dynamic, a static select statement
				// cannot be used. Instead, we build a dynamic one that waits on each worker's
				// error channel and a timeout, after which a new watchlist check is performed.

				cases := make([]reflect.SelectCase, 0, len(s.erc20wHandles)+1)

				// Add timeout case.
				cases = append(cases, reflect.SelectCase{
					Dir: reflect.SelectRecv,
					Chan: reflect.ValueOf(
						time.After(
							time.Duration(s.erc20WatchlistCheckInterval) * time.Millisecond)),
				})

				// Add errCh case for each handle.
				for _, handle := range s.erc20wHandles {
					cases = append(cases, reflect.SelectCase{
						Dir:  reflect.SelectRecv,
						Chan: reflect.ValueOf(handle.errCh),
					})
				}

				chosen, val, _ := reflect.Select(cases)

				// Timeout - proceed to next watchlist check.
				if chosen == 0 {
					continue
				}

				errVal, _ := val.Interface().(struct {
					Err error
					Id  string
				})

				s.logErr("ERC-20 worker for token %s encountered a fatal error: %s",
					*s.erc20wHandles[strings.Split(errVal.Id, ":")[0]].token.Symbol,
					errVal.Err.Error())

				delete(s.erc20wHandles, strings.Split(errVal.Id, ":")[0])

				shutDownFn()

				return
			}
		}()
	}

	if s.dataAnchorBackend != nil {
		go func() {
			defer wg.Done()

			s.runDataAnchorWorkerController()
		}()
	}

	// EOA activity worker controller goroutine - responsible for managing the EOA activity
	// worker lifecycle. It has two tasks/responsibilities:
	//
	// 1. Listens for fatal errors from the EOA activity worker. Since a value sent to errCh
	//    indicates the EOA activity worker has already shut down, it logs the error and signals
	//    the other worker controllers to shut down as well.
	//
	// 2. Listens for a shutdown signal from the other worker controllers. Upon receiving it,
	//    it signals the eoa activity worker to stop by closing ctrlCh, and waits for it to
	//    shut down gracefully via doneCh.
	if s.eoaActivityBackend != nil {
		go func() {
			defer wg.Done()

			select {
			case err := <-s.eoaawHandle.errCh:
				s.logErr("EOA activity worker encountered a fatal error: %s", err.Err.Error())

				s.shutDownHandles()
			case <-s.shutDownCh:
				close(s.eoaawHandle.ctrlCh)

				select {
				case err := <-s.eoaawHandle.errCh:
					s.logErr("EOA activity worker encountered a fatal error: %s", err.Err.Error())
				case <-s.eoaawHandle.doneCh:
				}
			}
		}()
	}

	// ESG aggregation worker controller goroutine - responsible for managing the ESG aggregation
	// worker lifecycle. It has two tasks/responsibilities:
	//
	// 1. Listens for fatal errors from the ESG aggregation worker. Since a value sent to errCh
	//    indicates the ESG aggregation worker has already shut down, it logs the error and signals
	//    the other worker controllers to shut down as well.
	//
	// 2. Listens for a shutdown signal from the other worker controllers. Upon receiving it,
	//    it signals the eoa activity worker to stop by closing ctrlCh, and waits for it to
	//    shut down gracefully via doneCh.
	if s.esgAggregationBackend != nil {
		go func() {
			defer wg.Done()

			select {
			case err := <-s.esgAggregationWorkerHandle.errCh:
				s.logErr("ESG aggregation worker encountered a fatal error: %s", err.Err.Error())

				s.shutDownHandles()
			case <-s.shutDownCh:
				close(s.esgAggregationWorkerHandle.ctrlCh)

				select {
				case err := <-s.esgAggregationWorkerHandle.errCh:
					s.logErr("ESG aggregation worker encountered a fatal error: %s", err.Err.Error())
				case <-s.esgAggregationWorkerHandle.doneCh:
				}
			}
		}()
	}

	// [SCHEDULED FOR REMOVAL]
	// Tx pool worker controller goroutine - responsible for managing the tx pool worker lifecycle.
	// It has two tasks/responsibilities:
	//
	// 1. Listens for fatal errors from the tx pool worker. Since a value sent to errCh indicates
	//    the tx pool worker has already shut down, it logs the error and signals the transaction
	//    and block worker controllers to shut down as well.
	//
	// 2. Listens for a shutdown signal from the other two worker controllers. Upon receiving it,
	//    it signals the tx pool worker to stop by closing ctrlCh, and waits for it to shut down
	//    gracefully via doneCh.
	if s.syncTxPool {
		go func() {
			defer wg.Done()

			select {
			case err := <-s.txpwHandle.errCh:
				s.logErr("tx pool worker encountered a fatal error: %s", err.Error())

				s.shutDownHandles()
			case <-s.shutDownCh:
				close(s.txpwHandle.ctrlCh)

				select {
				case err := <-s.txpwHandle.errCh:
					s.logErr("tx pool worker encountered a fatal error: %s", err.Error())

					s.shutDownHandles()
				case <-s.txpwHandle.doneCh:
				}
			}
		}()
	}

	if s.metricsEnabled {
		srv := s.startMetricsServer()
		defer srv.Close() //nolint:errcheck

		// Not part of wg: sampleMetrics only observes state and must never delay shutdown.
		go s.sampleMetrics()
	}

	wg.Wait()

	return nil
}

// dialRPC opens an RPC client to the node, wrapping the HTTP transport with otelhttp so
// outbound requests carry the active span's W3C traceparent. This is unconditional: header
// propagation costs nothing when no collector is configured, and it is what lets the node
// join the syncer's trace even in environments that do not export spans themselves.
func (s *Syncer) dialRPC(rpcURL string) (*rpc.Client, error) {
	httpClient := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

	return rpc.DialOptions(context.Background(), rpcURL, rpc.WithHTTPClient(httpClient))
}

// startMetricsServer serves the Prometheus metrics at /metrics on [Syncer.metricsAddr]. The
// returned server is closed by [Syncer.Start] on shutdown.
func (s *Syncer) startMetricsServer() *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())

	srv := &http.Server{
		Addr:              s.metricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		s.log("metrics endpoint listening on %s/metrics", s.metricsAddr)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logWarn("metrics server stopped: %v", err)
		}
	}()

	return srv
}

// sampleMetrics periodically publishes observable metrics that cannot be updated
// inline at an event boundary: the node chain head / indexing lag and the depths
// of the inter-worker queues. It runs until [Syncer.shutDownCh] is closed.
func (s *Syncer) sampleMetrics() {
	ticker := time.NewTicker(time.Duration(s.pollInterval) * time.Millisecond)
	defer ticker.Stop()

	// Dedicated client for the head query so it never contends with the workers'
	// clients. If it cannot be established, queue-depth sampling still proceeds and
	// the dial is retried on each sample tick until it succeeds.
	var headClient *rpc.Client

	defer func() {
		if headClient != nil {
			headClient.Close()
		}
	}()

	sample := func() {
		s.m.Lock()
		blockCache := s.l.Len()
		s.m.Unlock()
		metrics.QueueDepth.WithLabelValues(metrics.QueueBlockCache).Set(float64(blockCache))

		metrics.QueueDepth.WithLabelValues(metrics.QueueTxJobs).Set(float64(s.txJobsInFlight.Load()))

		lastIndexed := s.lastIndexedBlock.Load()
		metrics.LastIndexedBlock.Set(float64(lastIndexed))

		if headClient == nil {
			client, err := s.dialRPC(s.rpcURL)
			if err != nil {
				s.logWarn("metrics sampler: cannot dial rpc for chain head: %v", err)

				return
			}

			headClient = client
		}

		var head hexutil.Uint64

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		callErr := headClient.CallContext(ctx, &head, "eth_blockNumber")

		cancel()

		if callErr != nil {
			s.logWarn("metrics sampler: cannot fetch chain head: %v", callErr)

			return
		}

		chainHead := uint64(head)
		metrics.ChainHeadBlock.Set(float64(chainHead))

		lag := int64(chainHead) - int64(lastIndexed)
		if lag < 0 {
			lag = 0
		}

		metrics.IndexingLagBlocks.Set(float64(lag))
	}

	sample()

	for {
		select {
		case <-s.shutDownCh:
			return
		case <-ticker.C:
			sample()
		}
	}
}

func (s *Syncer) shutDownHandles() {
	s.once.Do(func() {
		close(s.shutDownCh)
	})
}

// shutDown gracefully shuts down the syncer.
func (s *Syncer) shutDown() {
	s.bwHandle.client.Close()

	for _, handle := range s.txwHandles {
		handle.client.Close()
	}

	if s.txpwHandle != nil {
		s.txpwHandle.client.Close()
	}

	s.log("shut down")
}

// log records a lifecycle event at info level. Prefer [Syncer.logCtx] wherever a
// context is in scope: it adds the trace and span IDs that join the line to its trace.
func (s *Syncer) log(msg string, args ...any) {
	s.emit(context.Background(), slog.LevelInfo, msg, args...)
}

// logCtx records a lifecycle event at info level, tagged with the active span's IDs.
func (s *Syncer) logCtx(ctx context.Context, msg string, args ...any) {
	s.emit(ctx, slog.LevelInfo, msg, args...)
}

// logWarn reports a recoverable problem at warn level: something failed but the
// component is retrying or degrading rather than stopping.
func (s *Syncer) logWarn(msg string, args ...any) {
	s.emit(context.Background(), slog.LevelWarn, msg, args...)
}

// logWarnCtx reports a recoverable problem at warn level, tagged with the span's IDs.
func (s *Syncer) logWarnCtx(ctx context.Context, msg string, args ...any) {
	s.emit(ctx, slog.LevelWarn, msg, args...)
}

// logErr reports a failure at error level. Failures must not be logged through [Syncer.log]:
// info is filtered out in normal operation, which would make the process fail silently.
func (s *Syncer) logErr(msg string, args ...any) {
	s.emit(context.Background(), slog.LevelError, msg, args...)
}

// logErrCtx reports a failure at error level, tagged with the active span's IDs.
func (s *Syncer) logErrCtx(ctx context.Context, msg string, args ...any) {
	s.emit(ctx, slog.LevelError, msg, args...)
}

// emit is the single place that formats a message and attaches the component identity
// and trace correlation fields. The level check short-circuits before the Sprintf, which
// matters because every call site formats eagerly.
func (s *Syncer) emit(ctx context.Context, level slog.Level, msg string, args ...any) {
	// Tolerate a zero-value struct: tests construct these directly, and a missing
	// logger must not turn a log line into a nil dereference.
	logger := s.logger
	if logger == nil {
		logger = slog.Default()
	}

	if !logger.Enabled(ctx, level) {
		return
	}

	attrs := make([]any, 0, 8)
	attrs = append(attrs, "component", "syncer")
	attrs = append(attrs, tracing.LogFields(ctx)...)

	logger.Log(ctx, level, fmt.Sprintf(msg, args...), attrs...)
}

type blockWorkerHandle struct {
	bw         *blockworker.BlockWorker
	id         uint64
	ctrlCh     chan struct{}
	doneCh     chan struct{}
	errCh      chan error
	startBlock uint64
	client     *rpc.Client
}

func (s *Syncer) createBlockWorkerHandle(
	id uint64,
	client *rpc.Client,
	ctrlCh chan struct{},
	doneCh chan struct{},
	errCh chan error,
	startBlock uint64,
) (*blockWorkerHandle, error) {
	processBlockFn := func(block *types.Block) error {
		if err := s.storage.InsertBlock(block); err != nil {
			return err
		}

		s.addBlock(block)

		return nil
	}

	opts := []blockworker.BlockWorkerOption{
		blockworker.WithPollInterval(s.pollInterval),
		blockworker.WithRetry(s.maxRetries, s.retryInterval),
		blockworker.WithStartBlock(startBlock),
	}

	if s.logger != nil {
		opts = append(opts, blockworker.WithLogger(s.logger))
	}

	if s.tipOnly {
		opts = append(opts, blockworker.WithTipOnly())
	}

	if s.withTxs {
		opts = append(opts, blockworker.WithFullTransactions())
	}

	bw, err := blockworker.NewBlockWorker(
		// Wrap the client so each node RPC call is timed into
		// syncer_node_rpc_duration_seconds. The handle keeps the raw client for
		// connection lifecycle (Close).
		metrics.NewInstrumentedRPCClient(client),
		processBlockFn,
		ctrlCh,
		doneCh,
		errCh,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create block worker: %w", err)
	}

	return &blockWorkerHandle{
		bw,
		0,
		ctrlCh,
		doneCh,
		errCh,
		startBlock,
		client,
	}, nil
}

type txPoolWorkerHandle struct {
	txpw   *txpoolworker.TxPoolWorker
	id     uint64
	ctrlCh chan struct{}
	doneCh chan struct{}
	errCh  chan error
	client *rpc.Client
}

func (s *Syncer) createTxPoolWorkerHandle(
	id uint64,
	client *rpc.Client,
	ctrlCh chan struct{},
	doneCh chan struct{},
	errCh chan error,
) (*txPoolWorkerHandle, error) {
	processTxPoolFn := func(pending, queued []*types.Transaction) error {
		if err := s.storage.InsertPoolTransactions(pending, queued); err != nil {
			return err
		}

		return nil
	}

	opts := []txpoolworker.TxPoolWorkerOption{
		txpoolworker.WithPollInterval(s.txPoolPollInterval),
		txpoolworker.WithRetry(s.maxRetries, s.retryInterval),
	}

	if s.logger != nil {
		opts = append(opts, txpoolworker.WithLogger(s.logger))
	}

	txpw, err := txpoolworker.NewTxPoolWorker(
		client,
		processTxPoolFn,
		ctrlCh,
		doneCh,
		errCh,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create tx pool worker: %w", err)
	}

	return &txPoolWorkerHandle{
		txpw,
		0,
		ctrlCh,
		doneCh,
		errCh,
		client,
	}, nil
}

type txWorkerHandle struct {
	txw    *txworker.TxWorker
	id     uint64
	jobCh  chan txworker.TxJob
	doneCh chan uint64
	errCh  chan struct {
		Err error
		Id  uint64
	}
	startBlock uint64
	client     *rpc.Client
}

func (s *Syncer) createTxWorkerHandle(
	id uint64,
	client *rpc.Client,
	jobCh chan txworker.TxJob,
	doneCh chan uint64,
	errCh chan struct {
		Err error
		Id  uint64
	},
	startBlock uint64,
) (*txWorkerHandle, error) {
	processTxsFn := func(txs []*types.Transaction) error {
		// TODO: write explanation
		return nil
	}

	fetchTxDataFn := func(hash string) bool {
		if s.withTxs {
			return false
		}

		return s.storage.ShouldFetchFullTransaction(hash)
	}

	opts := []txworker.TxWorkerOption{
		txworker.WithID(id),
		txworker.WithRetry(s.maxRetries, s.retryInterval),
		txworker.WithBatchSize(s.batchSize),
	}

	if s.logger != nil {
		opts = append(opts, txworker.WithLogger(s.logger))
	}

	txw, err := txworker.NewTxWorker(
		// Wrap the client so each (batch) node RPC call is timed into
		// syncer_node_rpc_duration_seconds. The handle keeps the raw client for
		// connection lifecycle (Close).
		metrics.NewInstrumentedRPCClient(client),
		processTxsFn,
		fetchTxDataFn,
		doneCh,
		jobCh,
		errCh,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create tx worker: %w", err)
	}

	return &txWorkerHandle{
		txw,
		id,
		jobCh,
		doneCh,
		errCh,
		startBlock,
		client,
	}, nil
}

type erc20WorkerHandle struct {
	erc20w *prologworker.PrologWorker
	token  *types.ERC20Token
	ctrlCh chan struct{}
	doneCh chan string
	errCh  chan struct {
		Err error
		Id  string
	}
}

func (s *Syncer) createErc20WorkerHandle(
	token *types.ERC20Token,
	ctrlCh chan struct{},
	doneCh chan string,
	errCh chan struct {
		Err error
		Id  string
	},
) (*erc20WorkerHandle, error) {
	// In a standard workflow, getBlockFn would return a full block containing all transactions
	// and their respective receipts (logs), leaving the worker to filter them.
	//
	// To optimize performance, we leverage database indexing via [Erc20Backend.GetLogs] to fetch
	// only the relevant logs, bypassing the need for the worker to scan every log in the block.
	//
	// We "trick" the worker by:
	// 1. returning a block with a single dummy transaction containing our pre-filtered logs, and
	// 2. passing a 'nil' filter to the worker's constructor, ensuring it forwards all logs from
	//    the block (that is, our dummy transaction) directly to processLogsFn without additional
	//    overhead.
	getBlockFn := func(blockNum uint64) (*types.Block, error) {
		tip, err := s.erc20Backend.GetTip()
		if err != nil {
			return nil, err
		}

		// GetTip returns 0 both when not even one block has been processed yet and when the last
		// processed block is the genesis block. To avoid ambiguity, we wait until at least block
		// 1 is available.
		if tip == 0 {
			return nil, nil
		}

		// Wait if the block hasn't been processed by the main syncer yet.
		if tip < blockNum {
			return nil, nil
		}

		block, err := s.erc20Backend.GetBlock(blockNum)
		if err != nil {
			return nil, err
		}

		// Fetch only Transfer logs (covers mint, burn, and transfer events). Topic: Keccak-256
		// of "Transfer(address,address,uint256)".
		transferTopic := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

		logs, err := s.erc20Backend.GetLogs(blockNum, token.Address, []string{transferTopic})
		if err != nil {
			return nil, err
		}

		// Encapsulate logs in a dummy transaction. This satisfies the worker's expectation that
		// logs are tied to block transactions, while bypassing further filtering.
		block.Transactions = []*types.Transaction{
			{Hash: "dummy", Logs: logs},
		}

		return block, nil
	}

	processLogsFn := func(block *types.Block, logs []*types.ReceiptLog) error {
		if token.IsPrivate {
			// TODO: handle private tokens
			return nil
		}

		// We don't want to process genesis block.
		if block.Number == 0 {
			return nil
		}

		counts := map[string]uint64{
			"transfer": 0,
			"mint":     0,
			"burn":     0,
		}

		volumes := map[string]*big.Int{
			"transfer": big.NewInt(0),
			"mint":     big.NewInt(0),
			"burn":     big.NewInt(0),
		}

		for _, log := range logs {
			from, to, value, ok := helper.DecodeTransferLog(log.Topics, log.Data)
			if !ok {
				s.log("unexpected non-Transfer log for token, skipping")

				continue
			}

			if from == helper.ZeroAddr && to == helper.ZeroAddr {
				continue
			}

			class := helper.ClassifyTransfer(from, to)

			counts[class]++
			volumes[class].Add(volumes[class], value)
		}

		hour := time.Unix(int64(block.Timestamp), 0).UTC().Truncate(time.Hour)

		return s.erc20Backend.ProcessHourlyStat(
			uint64(block.Number),
			token,
			hour,
			counts,
			volumes)
	}

	opts := []prologworker.PrologWorkerOption{
		prologworker.WithID(token.Address + ":" + *token.Symbol),
		prologworker.WithStartBlock(token.NextBlock),
		prologworker.WithProcessInterval(s.erc20ProcessInterval),
		prologworker.WithWaitOnlyOnNil(),
	}

	if s.logger != nil {
		opts = append(opts, prologworker.WithLogger(s.logger))
	}

	erc20w, err := prologworker.NewPrologWorker(
		getBlockFn,
		processLogsFn,
		nil, // nil because GetLogs inside getBlockFn already pre-filters the data.
		ctrlCh,
		doneCh,
		errCh,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create ERC-20 worker: %w", err)
	}

	return &erc20WorkerHandle{
		erc20w,
		token,
		ctrlCh,
		doneCh,
		errCh,
	}, nil
}

type dataAnchorWorkerHandle struct {
	worker  *prologworker.PrologWorker
	factory *types.DataAnchorFactory
	ctrlCh  chan struct{}
	doneCh  chan string
	errCh   chan struct {
		Err error
		Id  string
	}
}

func (s *Syncer) runDataAnchorWorkerController() {
	shutDownFn := func() {
		s.shutDownHandles()
		s.stopDataAnchorWorkers()
	}

	var lastEmptyWatchlistLog time.Time

	for {
		select {
		case <-s.shutDownCh:
			shutDownFn()

			return
		default:
		}

		factories, err := s.dataAnchorBackend.GetWatchlist()
		if err != nil {
			s.logWarn("failed to fetch data-anchor factory watchlist: %s", err.Error())
			shutDownFn()

			return
		}

		enabled := 0

		for _, factory := range factories {
			if factory != nil && factory.Enabled {
				enabled++
			}
		}

		if enabled == 0 && time.Since(lastEmptyWatchlistLog) >= 15*time.Second {
			lastEmptyWatchlistLog = time.Now().UTC()

			s.log("data-anchor watchlist has no enabled factories "+
				"(register via POST /admin/v1/data-anchor/factories); active_workers=%d",
				len(s.dataAnchorwHandles))
		}

		if err := s.reconcileDataAnchorWorkers(factories); err != nil {
			s.logWarn("failed to reconcile data-anchor workers: %s", err.Error())
			shutDownFn()

			return
		}

		cases := []reflect.SelectCase{
			{
				Dir: reflect.SelectRecv,
				Chan: reflect.ValueOf(time.After(
					time.Duration(s.dataAnchorWatchlistCheckInterval) * time.Millisecond)),
			},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.shutDownCh)},
		}

		handles := make([]*dataAnchorWorkerHandle, 0, len(s.dataAnchorwHandles))
		for _, handle := range s.dataAnchorwHandles {
			handles = append(handles, handle)
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(handle.errCh),
			})
		}

		chosen, value, _ := reflect.Select(cases)
		if chosen == 0 {
			continue
		}

		if chosen == 1 {
			shutDownFn()

			return
		}

		handle := handles[chosen-2]
		workerErr, _ := value.Interface().(struct {
			Err error
			Id  string
		})

		delete(s.dataAnchorwHandles, handle.factory.Address)

		if errors.Is(workerErr.Err, types.ErrDataAnchorCursorChanged) {
			s.log("data-anchor worker for factory %s stopped after its cursor changed; restarting",
				handle.factory.Address)

			continue
		}

		s.logErr("data-anchor worker for factory %s encountered a fatal error: %s",
			handle.factory.Address, workerErr.Err.Error())
		shutDownFn()

		return
	}
}

func (s *Syncer) createDataAnchorWorkerHandle(
	factory *types.DataAnchorFactory,
) (*dataAnchorWorkerHandle, error) {
	ctrlCh := make(chan struct{}, 1)
	doneCh := make(chan string, 1)
	errCh := make(chan struct {
		Err error
		Id  string
	}, 1)

	var lastTipWaitLog time.Time

	getBlockFn := func(blockNumber uint64) (*types.Block, error) {
		tip, err := s.dataAnchorBackend.GetTip()
		if err != nil {
			return nil, err
		}

		if tip == nil || *tip < blockNumber {
			// Rate-limit: at the tip this path runs every process interval.
			if time.Since(lastTipWaitLog) >= 15*time.Second {
				lastTipWaitLog = time.Now().UTC()

				if tip == nil {
					s.log("data-anchor worker %s waiting: tx worker tip not set yet (need block %d)",
						factory.Address, blockNumber)
				} else {
					s.log("data-anchor worker %s waiting: tx worker tip=%d < next_block=%d",
						factory.Address, *tip, blockNumber)
				}
			}

			return nil, nil
		}

		logs, err := s.dataAnchorBackend.GetLogs(blockNumber)
		if err != nil {
			return nil, err
		}

		return &types.Block{
			Number:       hexutil.Uint64(blockNumber),
			Transactions: []*types.Transaction{{Hash: "data-anchor", Logs: logs}},
		}, nil
	}

	processLogsFn := func(block *types.Block, logs []*types.ReceiptLog) error {
		entries := make([]types.ReceiptLog, 0, len(logs))
		for _, entry := range logs {
			if entry != nil {
				entries = append(entries, *entry)
			}
		}

		return s.dataAnchorBackend.ProcessBlock(uint64(block.Number), factory, entries)
	}

	opts := []prologworker.PrologWorkerOption{
		prologworker.WithID(factory.Address),
		prologworker.WithStartBlock(factory.NextBlock),
		prologworker.WithProcessInterval(s.dataAnchorProcessInterval),
		prologworker.WithWaitOnlyOnNil(),
	}
	if s.logger != nil {
		opts = append(opts, prologworker.WithLogger(s.logger))
	}

	worker, err := prologworker.NewPrologWorker(
		getBlockFn,
		processLogsFn,
		nil,
		ctrlCh,
		doneCh,
		errCh,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create data-anchor worker: %w", err)
	}

	handle := &dataAnchorWorkerHandle{
		worker:  worker,
		factory: factory,
		ctrlCh:  ctrlCh,
		doneCh:  doneCh,
		errCh:   errCh,
	}

	return handle, nil
}

func (s *Syncer) reconcileDataAnchorWorkers(factories []*types.DataAnchorFactory) error {
	activeFactories := make(map[string]*types.DataAnchorFactory, len(factories))
	for _, factory := range factories {
		if factory != nil && factory.Enabled {
			activeFactories[factory.Address] = factory
		}
	}

	for address := range s.dataAnchorwHandles {
		if _, active := activeFactories[address]; active {
			continue
		}

		if err := s.stopDataAnchorWorker(address); err != nil {
			return err
		}
	}

	for _, factory := range activeFactories {
		if _, exists := s.dataAnchorwHandles[factory.Address]; exists {
			continue
		}

		handle, err := s.createDataAnchorWorkerHandle(factory)
		if err != nil {
			return fmt.Errorf("create data-anchor worker for factory %s: %w",
				factory.Address, err)
		}

		if err := handle.worker.Start(); err != nil {
			return fmt.Errorf("start data-anchor worker for factory %s: %w",
				factory.Address, err)
		}

		s.dataAnchorwHandles[factory.Address] = handle
		s.log("data-anchor worker for factory %s started at block %d",
			factory.Address, factory.NextBlock)
	}

	return nil
}

func (s *Syncer) stopDataAnchorWorker(address string) error {
	handle, exists := s.dataAnchorwHandles[address]
	if !exists {
		return nil
	}

	delete(s.dataAnchorwHandles, address)
	close(handle.ctrlCh)

	select {
	case workerErr := <-handle.errCh:
		return fmt.Errorf("data-anchor worker for factory %s encountered a fatal error: %w",
			handle.factory.Address, workerErr.Err)
	case <-handle.doneCh:
		s.log("data-anchor worker for factory %s successfully shut down",
			handle.factory.Address)

		return nil
	}
}

func (s *Syncer) stopDataAnchorWorkers() {
	for address := range s.dataAnchorwHandles {
		if err := s.stopDataAnchorWorker(address); err != nil {
			s.logErr("%s", err.Error())
		}
	}
}

type eoaActivityWorkerHandle struct {
	eoaaw  *abstractworker.AbstractWorker
	ctrlCh chan struct{}
	doneCh chan string
	errCh  chan struct {
		Err error
		Id  string
	}
}

func (s *Syncer) createEoaActivityWorkerHandle() (*eoaActivityWorkerHandle, error) {
	ctrlCh := make(chan struct{}, 1)
	doneCh := make(chan string, 1)
	errCh := make(chan struct {
		Err error
		Id  string
	}, 1)

	client, err := s.dialRPC(s.rpcURL)
	if err != nil {
		return nil, fmt.Errorf("cannot establish RPC connection for eoa activity worker: %w", err)
	}

	currentBlock := s.eoaActivityStartBlock

	processFn := func(log func(string, ...any)) (done bool, wait bool, err error) {
		log("processing block %v", currentBlock)

		participants, err := s.eoaActivityBackend.GetBlockParticipants(currentBlock)
		if err != nil {
			return false, false, err
		}

		if participants == nil {
			return false, true, nil
		}

		eoaAddresses := make([]string, 0, len(participants))
		toAddresses := make([]string, 0, len(participants))

		for _, participant := range participants {
			eoaAddresses = append(eoaAddresses, participant.From)

			if participant.To != nil {
				toAddresses = append(toAddresses, *participant.To)
			}
		}

		knownEOAs, err := s.eoaActivityBackend.FilterKnownEOAs(toAddresses)
		if err != nil {
			return false, false, err
		}

		knownSet := make(map[string]struct{}, len(knownEOAs))

		for _, addr := range knownEOAs {
			knownSet[addr] = struct{}{}
		}

		for _, addr := range toAddresses {
			if _, ok := knownSet[addr]; ok {
				eoaAddresses = append(eoaAddresses, addr)

				continue
			}

			var code hexutil.Bytes

			for i := int64(1); ; i++ {
				if err := client.CallContext(context.TODO(),
					&code,
					"eth_getCode",
					addr,
					"latest"); err != nil {
					log("failed to get code: %v", err.Error())

					if i == s.maxRetries {
						log("giving up...")

						return true, false, fmt.Errorf("failed to get code: %w", err)
					}

					select {
					case <-s.shutDownCh:
						return true, false, nil
					case <-time.After(time.Duration(s.retryInterval) * time.Millisecond):
						select {
						case <-s.shutDownCh:
							return true, false, nil
						default:
							continue
						}
					}
				}

				break
			}

			if len(code) == 0 {
				eoaAddresses = append(eoaAddresses, addr)
			}
		}

		if err := s.eoaActivityBackend.RecordEOAActivity(currentBlock, eoaAddresses); err != nil {
			return false, false, err
		}

		log("block %d processed, recorded %d EOA addresses", currentBlock, len(eoaAddresses))

		currentBlock++

		return false, false, nil
	}

	opts := []abstractworker.AbstractWorkerOption{
		abstractworker.WithID("0"),
		abstractworker.WithWorkerType("eoa activity"),
		abstractworker.WithProcessInterval(s.eoaActivityProcessInterval),
	}

	if s.logger != nil {
		opts = append(opts, abstractworker.WithLogger(s.logger))
	}

	eoaaw, err := abstractworker.NewAbstractWorker(
		processFn,
		ctrlCh,
		doneCh,
		errCh,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create eoa activity worker: %w", err)
	}

	return &eoaActivityWorkerHandle{
		eoaaw,
		ctrlCh,
		doneCh,
		errCh,
	}, nil
}

type esgAggregationWorkerHandle struct {
	worker *abstractworker.AbstractWorker
	ctrlCh chan struct{}
	doneCh chan string
	errCh  chan struct {
		Err error
		Id  string
	}
}

func (s *Syncer) createESGAggregationWorkerHandle(
	ctx context.Context,
) (*esgAggregationWorkerHandle, error) {
	ctrlCh := make(chan struct{}, 1)
	doneCh := make(chan string, 1)
	errCh := make(chan struct {
		Err error
		Id  string
	}, 1)

	processFn := func(log func(string, ...any)) (done bool, wait bool, err error) {
		return s.esgAggregationBackend.Process(ctx, log)
	}

	opts := []abstractworker.AbstractWorkerOption{
		abstractworker.WithID("0"),
		abstractworker.WithWorkerType("esg aggregation"),
		abstractworker.WithProcessInterval(s.esgAggregationPollInterval),
	}

	if s.logger != nil {
		opts = append(opts, abstractworker.WithLogger(s.logger))
	}

	esgaw, err := abstractworker.NewAbstractWorker(
		processFn,
		ctrlCh,
		doneCh,
		errCh,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create esg aggregation worker: %w", err)
	}

	return &esgAggregationWorkerHandle{
		esgaw,
		ctrlCh,
		doneCh,
		errCh,
	}, nil
}

func (s *Syncer) addBlock(block *types.Block) {
	s.m.Lock()

	s.l.PushBack(block)

	select {
	case s.s <- struct{}{}:
	default:
	}

	s.m.Unlock()
}

func (s *Syncer) getBlock() *types.Block {
	s.m.Lock()

	for s.l.Len() == 0 {
		s.m.Unlock()

		// The window (space/time) between s.m.Unlock() and <-s.s is why s must be buffered.
		select {
		case <-s.s:
			select {
			case <-s.shutDownCh:
				return nil
			default:
			}

			s.m.Lock()
		case <-s.shutDownCh:
			return nil
		}
	}

	front := s.l.Front()
	block, _ := s.l.Remove(front).(*types.Block)

	s.m.Unlock()

	return block
}
