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

**Explorer queries** filter transactions by the same **whitelisted `data_method` selectors** as ucl-node2 (see `api_storage/api_storage.go`). Only transactions whose input starts with one of those selectors appear in block txn counts and transaction lists. The indexer stores `data_method` as the first 10 characters of `input` (`0x` + 8 hex).

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

### Explorer API (HTTP — same as polygon-edge `jsonrpc.JSONRPC`)

| Method | Path | Behavior |
|--------|------|------------|
| `POST` | `/` | JSON-RPC 2.0 body; use `explorer_*` methods (params as JSON array). |
| `GET` | `/` (or any path without a more specific handler) | JSON `{ "name", "chain_id", "version" }` — same shape as polygon-edge `GetResponse`. |
| `GET` | `/health` | `{"status":"ok"}` — optional liveness probe (not part of polygon-edge, does not change JSON-RPC behavior). |
| `GET` | `/ws` | Polygon-edge upgrades WebSocket; this binary responds **501 Not Implemented** (no filter subscriptions). |

```bash
./ucl-block-explorer-syncer api \
  --listen 0.0.0.0:8545 \
  --db-conn 'postgres://USER:PASSWORD@localhost:5432/DBNAME?sslmode=disable' \
  --chain-name "my-chain" \
  --chain-id 100
```

**`explorer_*` methods:** `explorer_getBlockList`, `explorer_getBlockDetail`, `explorer_getLineData`, `explorer_getTransactionList`, `explorer_getTransactionByHash`, `explorer_getBlockTransactionCount`.

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
