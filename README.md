# ucl-block-explorer-syncer

Blockchain indexer that reads blocks and transactions from an **EVM-compatible JSON-RPC** node (including **Polygon Edge**) and writes them into **PostgreSQL**. The **`api`** command exposes the same **HTTP layout as polygon-edge** (`ucl-node2`): **`POST /`** for JSON-RPC (including `explorer_*`), **`GET /`** for `{name, chain_id, version}`, and **`/ws`** registered (this service returns 501 for WS). There are **no `/api/...` REST routes** on the node—explorer data is only via JSON-RPC on **`POST /`** (see `INDEXER_AND_EXPLORER_API.md` in ucl-node2).

The syncer runs three logical pipelines concurrently:

- **Block worker** — polls the node for new blocks and stores them (and tx hashes or full txs, depending on flags).
- **Transaction workers** — fetch receipts (and optionally full tx data) to complete each transaction record.
- **Optional tx pool worker** — indexes pending and queued txs from `txpool_content` when enabled.

On restart, the process resumes from the last indexed block in the database.

## Requirements

- **Go** 1.24+ (see `go.mod`; the toolchain may auto-download a newer Go if needed).
- **PostgreSQL** with the schema applied from `scripts/init.sql` (schema `chain`, tables `blocks`, `transactions`, `metadata`).
- A reachable **HTTP JSON-RPC** endpoint for your chain (Polygon Edge default is often port `8545`).

## Database setup

Create a database and apply the schema:

```bash
psql "$DATABASE_URL" -f scripts/init.sql
```

Or use the Docker Compose setup below, which mounts `scripts/init.sql` on first PostgreSQL startup.

**Explorer queries** return transactions from `chain.transactions` without filtering by `data_method`. The indexer stores `data_method` as the first 10 characters of `input` (`0x` + 8 hex) for display and optional client-side use.

## CLI commands

The binary exposes two subcommands:

| Command | Purpose |
|---------|---------|
| `sync` (alias `syncer`) | Run the block/tx indexer into Postgres. |
| `api` | Serve explorer JSON-RPC on **`POST /`** (read-only Postgres; HTTP matches polygon-edge). |

```bash
go build -o ucl-block-explorer-syncer .
./ucl-block-explorer-syncer --help
./ucl-block-explorer-syncer sync --help
./ucl-block-explorer-syncer api --help
```

### Sync (indexer)

```bash
./ucl-block-explorer-syncer sync \
  --rpc-url http://127.0.0.1:8545 \
  --db-conn 'postgres://USER:PASSWORD@localhost:5432/DBNAME?sslmode=disable'
```

| Flag | Short | Description |
|------|--------|-------------|
| `--rpc-url` | `-r` | JSON-RPC URL of the node (**required**). |
| `--db-conn` | `-c` | PostgreSQL connection string (**required**). |
| `--logging` | `-v` | Verbose logs to stdout. |
| `--poll-interval` | | Block poll interval in ms (default `2000`, min `200`). |
| `--tip-only` | | Apply poll interval only after catching up to the chain tip. |
| `--full-block` | `-f` | Fetch full tx objects in each block. |
| `--batch-size` | `-b` | RPC batch size for tx fetching (default `1`). |
| `--tx-workers` | `-w` | Concurrent tx fetch workers (default `1`). |
| `--sync-tx-pool` | | Index mempool txs via `txpool_content`. |
| `--tx-pool-poll-interval` | | Mempool poll interval in ms (default `2000`). |
| `--erc20-stats` | | After each committed block, decode ERC-20 `Transfer` logs for tokens in `chain.erc20_watchlist` and upsert **UTC-hour** counts/volumes in `chain.erc20_hourly_stats` (non-blocking queue; requires receipt logs from the node). |
| `--erc20-stats-buffer` | | Max queued ERC-20 block jobs (default `64`); if full, newer jobs are dropped and logged. |
| `--entity-stats` | | After each block, upsert `chain.entity_hour_participation` (active EOAs per hour) and `chain.eoa_first_seen` (onboarding EOAs). Both exclude contract addresses using `eth_getCode` at the block. Requires **`--full-block`** so `from`/`to` are stored. |
| `--entity-stats-buffer` | | Max queued entity-stats jobs (default `64`); if full, blocks are dropped and logged. |

**ERC-20 stats:** Populate `chain.erc20_watchlist` with token contract addresses (`enabled = true`). The indexer classifies **mint** (`from` = zero address), **burn** (`to` = zero address), and **transfer** (otherwise) using standard `Transfer` events only—not internal calls without logs. Volumes are raw uint256 sums (`NUMERIC` strings in the API). Buckets are **UTC hours** from the block timestamp (`erc20_hourly_stats.hour_utc`).

### Explorer API (HTTP — same as polygon-edge `jsonrpc.JSONRPC`)

| Method | Path | Behavior |
|--------|------|------------|
| `POST` | `/` | JSON-RPC 2.0 body; use `explorer_*` methods (params as JSON array). |
| `GET` | `/` (or any path without a more specific handler) | JSON `{ "name", "chain_id", "version" }` — same shape as polygon-edge `GetResponse`. |
| `GET` | `/health` | `{"status":"ok"}` — optional liveness probe (not part of polygon-edge, does not change JSON-RPC behavior). |
| `GET` | `/ws` | Polygon-edge upgrades WebSocket; this binary responds **501 Not Implemented** (no filter subscriptions). |
| `POST` | `/admin/v1/erc20/watchlist` | **Optional:** upsert a row in `chain.erc20_watchlist`. Requires header `Authorization: Bearer <token>` where the token is **`ADMIN_API_SECRET`** (env) or **`--admin-api-secret`**. If the secret is not configured, responds **404**. JSON body: `address` (required, hex), optional `symbol`, `decimals`, `enabled` (default `true`). |

```bash
./ucl-block-explorer-syncer api \
  --listen 0.0.0.0:8545 \
  --db-conn 'postgres://USER:PASSWORD@localhost:5432/DBNAME?sslmode=disable' \
  --chain-name "my-chain" \
  --chain-id 100
```

**`explorer_*` methods:** `explorer_getBlockList`, `explorer_getBlockDetail`, `explorer_getLineData`, `explorer_getTransactionList`, `explorer_getTransactionByHash`, `explorer_getBlockTransactionCount`, `explorer_getErc20DailyStats`, `explorer_getErc20CirculationCumulative`, `explorer_getErc20Watchlist`, `explorer_getActiveEntityDailyStats`, `explorer_getOnboardingEntityDailyStats`.

**Entity / adoption stats:** JSON `fromDay`, `toDay`, optional `fromUtc`/`toUtc` (RFC3339), optional **`granularity`**: `hour` \| `day` \| `month` (default `day`), `page`, `pageSize`. Response `data.list[]` has `bucketUtc`, `dayUtc`, and `count`. Hour granularity requires a bounded window (`fromUtc`/`toUtc` or both `fromDay` and `toDay`). Filled when the syncer runs with **`--entity-stats`** (and schema from `scripts/init.sql`).

**ERC-20 transfer stats (`explorer_getErc20DailyStats`):** Same optional `granularity` and time fields; aggregates from **`chain.erc20_hourly_stats`**.

**ERC-20 cumulative circulation:** `explorer_getErc20CirculationCumulative` — same optional `granularity` and time fields. Response `data.list[]` has `dayUtc`, `bucketUtc`, and `total` (decimal string, human units): cumulative mint−burn across all **enabled** watchlist tokens with **decimals** set, with iterative clamp at `0`. Past UTC hours are cached in **`chain.erc20_circulation_cumulative`** (one row per completed hour); the open **current** hour is computed live. Requests with more than **~25k calendar days** in the `fromDay`/`toDay` window return `400`; **`granularity=hour`** is limited to **~31 days** of hours. After reindexing or changing historical hourly stats / watchlist rows, run **`TRUNCATE chain.erc20_circulation_cumulative`** so the cache can rebuild.

**Note:** A URL like `GET http://localhost:8545/api/transaction/count` is **not** a special route on polygon-edge either—`GET` on any path is handled the same way and returns **chain metadata**, not transaction counts. Use **`POST /`** with `explorer_getTransactionList` or `explorer_getBlockTransactionCount` for counts.

## Run with Docker Compose

Compose starts **PostgreSQL**, the **sync** job, and the **api** service on port **8545** (explorer HTTP).

1. Copy the example env file and set your node’s JSON-RPC URL:

   ```bash
   cp .env.example .env
   # Edit .env: set RPC_URL to your Polygon Edge / EVM node (e.g. http://host.docker.internal:8545)
   ```

2. Start the stack:

   ```bash
   docker compose up --build
   ```

Point the frontend at `http://localhost:8545` for explorer JSON-RPC (or your published host/port). The syncer uses `RPC_URL` to reach the chain node; the API service only talks to Postgres.

The Compose **syncer** runs with **`--tx-workers`** (default **8**, override **`TX_WORKERS`** in `.env`), **`--full-block`**, **`--erc20-stats`**, and **`--entity-stats`**: full transactions are stored (needed for adoption stats), ERC-20 `Transfer` logs are aggregated per watchlist token, and per-hour **active** (EOAs only) / **onboarding** entity tables are updated (`eth_getCode` excludes contracts for both). Populate **`chain.erc20_watchlist`** for tokens you care about (SQL, or set **`ADMIN_API_SECRET`** in `.env` and use **`POST /admin/v1/erc20/watchlist`** on the API); without watchlist rows the ERC-20 worker no-ops. Override queue depth with **`ERC20_STATS_BUFFER`** or **`ENTITY_STATS_BUFFER`** in `.env` if needed.

## Project layout

| Path | Role |
|------|------|
| `main.go` | CLI entrypoint. |
| `cli/` | Cobra: `sync` and `api` subcommands. |
| `syncer/` | Block, tx, and optional tx-pool workers. |
| `storage_handler/` | PostgreSQL persistence (`chain` schema). |
| `api_storage/` | Explorer SQL + request/response types (ported from ucl-node2). |
| `explorer/` | Thin handlers (same behavior as `jsonrpc/explorer_endpoint.go`). |
| `jsonrpc/` | Explorer-only JSON-RPC dispatch. |
| `httpserver/` | HTTP: `POST /` JSON-RPC, `GET /` metadata, `/ws` — aligned with `polygon-edge/jsonrpc/jsonrpc.go`. |
| `scripts/init.sql` | Database schema and indexes. |
