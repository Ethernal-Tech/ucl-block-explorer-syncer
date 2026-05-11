package syncer

import (
	"container/list"
	"context"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"

	abstractworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/abstract_worker"
	blockworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/block_worker"
	circulationworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/circulation_worker"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/helper"
	prologworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/prolog_worker"
	txworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/tx_worker"
	txpoolworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/txpool_worker"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
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

	// logger records state changes and actions during the syncer's lifecycle. By default, no
	// logging is performed.
	logger types.Logger

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

	// maxRetries is the maximum number of RPC request attempts for fetching blockchain data
	// (blocks, transactions, and receipts) before giving up. -1 denotes indefinitely. By default,
	// the first failure is treated as fatal.
	maxRetries int64

	// retryInterval specifies how long the syncer waits between two consecutive RPC attempts,
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

	// circulationPollInterval is how often (in milliseconds) the circulation worker
	// checks for and backfills new completed hours into chain.erc20_circulation_cumulative.
	circulationPollInterval uint64

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

	// eoaActivityBackend is the backend required for processing EOA activities. If nil, processing
	// is disabled. For details, see the [EoaActivityBackend] interface documentation.
	eoaActivityBackend EoaActivityBackend

	// eoaActivityProcessInterval specifies how long the syncer waits before retrying to process a
	// block for EOA activity statistics when it is not yet available. By default, 2000 milliseconds.
	eoaActivityProcessInterval uint64

	// eoaActivityStartBlock is the block number from which the EOA activity worker begin processing.
	// By default, 0.
	eoaActivityStartBlock uint64

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

	// eoaawHandle holds the handle for the EOA activity worker managed by the syncer.
	eoaawHandle *eoaActivityWorkerHandle

	// txpwHandle holds the handle for the transaction pool worker managed by the syncer.
	txpwHandle *txPoolWorkerHandle

	cwHandle *circulationWorkerHandle

	// shutDownCh is closed to signal all workers (that is, their controller goroutines) to shut
	// down gracefully.
	shutDownCh chan struct{}
	once       sync.Once
}

// NewSyncer constructs a new [Syncer] instance. rpcURL must be a valid RPC endpoint URL used
// to establish a connection to the EVM-based node. storage is the persistence layer used for
// indexed data, see [StorageHandler] for details. It cannot be nil.
//
// The following optional configurations are available (see their documentation for details):
//  1. WithLogger (default: no logging)
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
		rpcURL:                      rpcURL,
		storage:                     storage,
		maxRetries:                  1,
		retryInterval:               2000,
		batchSize:                   1,
		maxTxWorkers:                1,
		pollInterval:                2000,
		txPoolPollInterval:          2000,
		erc20WatchlistCheckInterval: 2000,
		erc20ProcessInterval:        2000,
		eoaActivityProcessInterval:  2000,
	}

	for _, o := range opts {
		if err := o(syncer); err != nil {
			return nil, err
		}
	}

	syncer.s = make(chan struct{}, 1)
	syncer.l = list.New()
	syncer.shutDownCh = make(chan struct{})

	// Block worker handle construction.
	{
		client, err := rpc.Dial(rpcURL)
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

		client, err := rpc.Dial(rpcURL)
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

	// EOA activity worker handle construction.
	if syncer.eoaActivityBackend != nil {
		eoaawh, err := syncer.createEoaActivityWorkerHandle()
		if err != nil {
			return nil, err
		}

		syncer.eoaawHandle = eoaawh
	}

	// [SCHEDULED FOR REMOVAL]
	// Transaction pool worker handle construction.
	if syncer.syncTxPool {
		client, err := rpc.Dial(rpcURL)
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

	//circulation worker
	{
		cwh, err := syncer.createCirculationWorkerHandle(
			0,
			make(chan struct{}, 1),
			make(chan struct{}, 1),
		)

		if err != nil {
			return nil, err
		}

		syncer.cwHandle = cwh
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

	if s.eoaActivityBackend != nil {
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
			s.log("block worker encountered a fatal error: %s", err.Error())

			s.shutDownHandles()
		case <-s.shutDownCh:
			close(s.bwHandle.ctrlCh)

			select {
			case err := <-s.bwHandle.errCh:
				s.log("block worker encountered a fatal error: %s", err.Error())
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

			for range len(s.txwHandles) - int(numOfAlreadyDown) {
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
					s.log("cannot get block %v: %s", currentBlock, err.Error())

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

			jobs := helper.CreateJobs(uint64(len(block.Transactions)), uint64(len(s.txwHandles)))

			s.log("%v jobs created", len(jobs))

			for i, job := range jobs {
				job.Block = block

				s.txwHandles[i].jobCh <- job

				s.log("job [%v-%v] dispatched", job.From, job.To)
			}

			l := len(jobs)
			errOcured := 0

			for l != 0 {
				select {
				case id := <-s.txwHandles[0].doneCh:
					s.log("tx worker %v finished", id)

					l--
				case err := <-s.txwHandles[0].errCh:
					s.log("transaction worker %v encountered a fatal error: %s", err.Id, err.Err.Error())

					errOcured++
					l--
				}
			}

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
				s.log("cannot insert transactions: %v", err.Error())

				shutDownFn(errOcured)

				break
			}

			s.log("block %v processed", currentBlock)

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
						s.log("ERC-20 worker for token %s encountered a fatal error: %s",
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
					s.log("failed to fetch the token watchlist: %s", err.Error())

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
						s.log("ERC-20 worker for token %s encountered a fatal error: %s",
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
							s.log("failed to fetch the tip of the chain: %s", err.Error())

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
						s.log("failed to create ERC-20 worker for token %s: %s",
							*token.Symbol,
							err.Error())

						shutDownFn()

						return
					}

					if err := handle.erc20w.Start(); err != nil {
						s.log("failed to start ERC-20 worker for token %s: %s",
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

				errVal := val.Interface().(struct {
					Err error
					Id  string
				})

				s.log("ERC-20 worker for token %s encountered a fatal error: %s",
					*s.erc20wHandles[strings.Split(errVal.Id, ":")[0]].token.Symbol,
					errVal.Err.Error())

				delete(s.erc20wHandles, strings.Split(errVal.Id, ":")[0])

				shutDownFn()

				return
			}
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
				s.log("EOA activity worker encountered a fatal error: %s", err.Err.Error())

				s.shutDownHandles()
			case <-s.shutDownCh:
				close(s.eoaawHandle.ctrlCh)

				select {
				case err := <-s.eoaawHandle.errCh:
					s.log("EOA activity worker encountered a fatal error: %s", err.Err.Error())
				case <-s.eoaawHandle.doneCh:
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
				s.log("tx pool worker encountered a fatal error: %s", err.Error())

				s.shutDownHandles()
			case <-s.shutDownCh:
				close(s.txpwHandle.ctrlCh)

				fmt.Println("TU SAM")

				select {
				case err := <-s.txpwHandle.errCh:
					s.log("tx pool worker encountered a fatal error: %s", err.Error())

					s.shutDownHandles()
				case <-s.txpwHandle.doneCh:
				}
			}
		}()
	}

	wg.Wait()

	return nil
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

func (s *Syncer) log(str string, args ...any) {
	if s.logger != nil {
		s.logger.Log(fmt.Sprintf("%s [syncer] %s",
			time.Now().Format("15:04:05.000"),
			fmt.Sprintf(str, args...)))
	}
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
		client,
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
		client,
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

	client, err := rpc.Dial(s.rpcURL)
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

			if err := client.CallContext(context.TODO(),
				&code,
				"eth_getCode",
				addr,
				"latest"); err != nil {
				return false, false, fmt.Errorf("failed to get code: %w", err)
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
	block := s.l.Remove(front).(*types.Block)

	s.m.Unlock()

	return block
}

type circulationWorkerHandle struct {
	cw     *circulationworker.CirculationWorker
	id     uint64
	doneCh chan struct{}
	crtlCh chan struct{}
}

func (s *Syncer) createCirculationWorkerHandle(
	id uint64,
	ctrlCh chan struct{},
	doneCh chan struct{},
) (*circulationWorkerHandle, error) {
	opts := []circulationworker.CirculationWorkerOption{}

	if s.circulationPollInterval != 0 {
		opts = append(
			opts, circulationworker.WithPollInterval(s.circulationPollInterval))
	}

	if s.logger != nil {
		opts = append(opts, circulationworker.WithLogger(s.logger))
	}

	if id != 0 {
		opts = append(opts, circulationworker.WithID(id))
	}

	cw, err := circulationworker.NewCirculationCacheWorker(s.entityDB, ctrlCh, doneCh, opts...)
	if err != nil {
		return nil, fmt.Errorf("cannot create circulation worker: %w", err)
	}

	return &circulationWorkerHandle{
		cw:     cw,
		id:     id,
		crtlCh: ctrlCh,
		doneCh: doneCh,
	}, nil
}
