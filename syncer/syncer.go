package syncer

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/rpc"

	blockworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/block_worker"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/helper"
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
	// only receipt-related fields of the transaction (i.e. Status) may contain any value, even
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
	// (i.e. Status) together with Hash, BlockHash, BlockNumber and BlockTimestamp are guaranteed
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

	// InsertPoolTransactions is invoked every time the current state of the transaction pool is
	// fetched, i.e. every time (pending and queued) transactions are retrieved and deserialized
	// from it. If the method returns an error, the syncer will terminate immediately.
	InsertPoolTransactions(pending, queued []*types.Transaction) error
}

// Syncer indexes an EVM-based blockchain by fetching and processing blocks and transactions via
// block and transaction workers, persisting the data to a storage backend. It supports different
// indexing strategies configurable through a set of constructor option functions (for example,
// [WithPollInterval]), that can be passed to [NewSyncer].
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

	// syncTxPool specifies whether the syncer should also fetch (pending) transactions from the
	// transaction pool. By default, false.
	syncTxPool bool

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

	// txpwHandle holds the handle for the transaction pool worker managed by the syncer.
	txpwHandle *txPoolWorkerHandle

	// shutDownBW is closed to signal block worker to shut down gracefully.
	shutDownBW chan struct{}

	// shutDownTXW is closed to signal all transaction workers to shut down gracefully.
	shutDownTXW chan struct{}

	// shutDownTXPW is closed to signal transaction pool worker to shut down gracefully.
	shutDownTXPW chan struct{}

	// closed indicates whether the shutdown channels have been closed and must only be accessed
	// while holding mClosed, as channels can only be closed once.
	closed  bool
	mClosed sync.Mutex
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
//  6. WithTxPool (default: disabled)
//  7. WithFullTransactions (default: false)
//  8. WithRetry (default: first failure is treated as fatal)
//  9. WithBatchSize (default: 1)
//  10. WithMaxTxWorkers (default: 1)
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
		rpcURL:             rpcURL,
		storage:            storage,
		maxRetries:         1,
		retryInterval:      2000,
		batchSize:          1,
		maxTxWorkers:       1,
		pollInterval:       2000,
		txPoolPollInterval: 2000,
	}

	for _, o := range opts {
		if err := o(syncer); err != nil {
			return nil, err
		}
	}

	syncer.s = make(chan struct{}, 1)
	syncer.l = list.New()
	syncer.shutDownBW = make(chan struct{})
	syncer.shutDownTXW = make(chan struct{})
	syncer.shutDownTXPW = make(chan struct{})

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

	return syncer, nil
}

// Start starts the syncer by launching the block and transaction workers. It returns an error
// if the syncer fails to start. Once running, the syncer operates until a fatal error occurs
// or it is stopped externally. For details on how the syncer orchestrates and manages its
// workers, see the detailed comments within this function.
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

	if s.syncTxPool {
		if err := s.txpwHandle.txpw.Start(); err != nil {
			return fmt.Errorf("cannot start block worker: %w", err)
		}
	}

	s.log("started")

	wg := sync.WaitGroup{}

	if s.syncTxPool {
		wg.Add(3)
	} else {
		wg.Add(2)
	}

	// Block worker controller goroutine - responsible for managing the block worker lifecycle.
	// It has two tasks/responsibilities:
	//
	// 1. Listens for fatal errors from the block worker. Since a value sent to errCh indicates
	//    the block worker has already shut down, it logs the error and signals the transaction
	//    and tx pool worker controllers to shut down as well.
	//
	// 2. Listens for a shutdown signal from the other two worker controllers. Upon receiving
	//    it, it signals the block worker to stop by closing ctrlCh, and waits for it to shut
	//    down gracefully via doneCh.
	go func() {
		defer wg.Done()

		select {
		case err := <-s.bwHandle.errCh:
			s.log("block worker encountered a fatal error: %s", err.Error())

			s.shutDownHandles()
		case <-s.shutDownBW:
			close(s.bwHandle.ctrlCh)

			<-s.bwHandle.doneCh
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
	//    a graceful shutdown of all other transaction workers and signals the block and tx pool
	// 	  worker controllers to shut down as well.
	//
	// 3. Listens for a shutdown signal from the other two worker controllers. Upon receiving it,
	//    initiates a graceful shutdown of all transaction workers.
	go func() {
		defer wg.Done()

		// shutDownFn signals the transaction workers (by closing their job channels) and the
		// other two worker controllers (by closing their shutDown channels) to shut down, and
		// waits for all active workers to shut down gracefully via doneCh. numOfAlreadyDown
		// indicates how many workers have already shut down (due to error) and should not be
		// waited on.
		shutDownFn := func(numOfAlreadyDown int) {
			s.shutDownHandles()

			for _, t := range s.txwHandles {
				close(t.jobCh)
			}

			for range len(s.txwHandles) - int(numOfAlreadyDown) {
				select {
				case <-s.txwHandles[0].doneCh:
				}
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
			case <-s.shutDownTXW:
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
			case <-s.shutDownTXPW:
				close(s.txpwHandle.ctrlCh)

				<-s.txpwHandle.doneCh
			}
		}()
	}

	wg.Wait()

	return nil
}

func (s *Syncer) shutDownHandles() {
	s.mClosed.Lock()

	if !s.closed {
		close(s.shutDownBW)
		close(s.shutDownTXW)
		close(s.shutDownTXPW)

		s.closed = true
	}

	s.mClosed.Unlock()
}

// shutDown gracefully shuts down the syncer.
func (s *Syncer) shutDown() {
	s.bwHandle.client.Close()

	for _, handle := range s.txwHandles {
		handle.client.Close()
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
			case <-s.shutDownTXW:
				return nil
			default:
			}

			s.m.Lock()
		case <-s.shutDownTXW:
			return nil
		}
	}

	front := s.l.Front()
	block := s.l.Remove(front).(*types.Block)

	s.m.Unlock()

	return block
}
