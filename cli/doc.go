package cli

var doc = `
Blockchain syncer/indexer that sequentially fetches and stores blocks and txs from an EVM
compatible JSON-RPC node into a PostgreSQL database. All workers described below operate
concurrently (e.g. the block worker does not wait for tx workers to finish processing its
txs before moving on to the next block). On restart, all workers resume from the block at
which they were stopped.

BLOCK WORKER
  Blocks are fetched sequentially from the EVM node (--rpc-url) by a single block worker.
  The worker polls for new blocks at a fixed interval defined by --poll-interval. By default,
  the poll interval applies continuously. When --tip-only is set, the interval is applied
  only after the block worker has caught up to the tip of the chain - until then, blocks
  are fetched as fast as possible without any delay.

  Depending on the --full-block flag, blocks are fetched either with full tx data or with
  tx hashes only. In both cases, their receipts are not included and are fetched separately
  by tx workers.

TRANSACTION WORKER(S)
  After each block is fetched, its txs are further processed by a pool of concurrent tx
  workers (--tx-workers). The number of workers defines the maximum concurrency. Thus, if
  a block, for example, contains fewer tx than the configured worker count, only as many
  workers as needed will be active.

  Tx data is fetched in batches of RPC calls (--batch-size). The number of RPC calls per
  transaction depends on --full-block:

  --full-block enabled:  1 RPC call per tx (getTransactionReceipt only, since other tx data
	                         was already fetched by the block worker)
  --full-block disabled: 2 RPC calls per tx (getTransactionByHash + getTransactionReceipt)

TX POOL WORKER
  An optional third worker can be enabled via --sync-tx-pool. When active, it periodically
  fetches pending and queued transactions from the node's transaction pool and indexes them
  into the database. The polling interval is controlled via --tx-pool-poll-interval.

LOGGING
  Verbose logging of all worker activity can be enabled with --logging (-v). All log output
  is written to stdout.

STORAGE
  All indexed data is persisted into a PostgreSQL database specified via --db-conn.`
