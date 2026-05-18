CREATE SCHEMA IF NOT EXISTS chain;

CREATE TABLE IF NOT EXISTS chain.blocks (
    hash VARCHAR(66) PRIMARY KEY,
    number BIGINT NOT NULL,
    parent_hash VARCHAR(66) NOT NULL,
    nonce VARCHAR(50) NOT NULL,
    sha3_uncles VARCHAR(66) NOT NULL,
    logs_bloom BYTEA NOT NULL,
    transactions_root VARCHAR(66) NOT NULL,
    state_root VARCHAR(66) NOT NULL,
    receipts_root VARCHAR(66) NOT NULL,
    miner VARCHAR(50) NOT NULL,
    difficulty BIGINT NOT NULL,
    total_difficulty BIGINT NOT NULL,
    extra_data TEXT NOT NULL,
    size BIGINT NOT NULL,
    gas_limit BIGINT NOT NULL,
    gas_used BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    mix_hash VARCHAR(66) NOT NULL,
    base_fee BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    txn_count BIGINT DEFAULT 0,
    CONSTRAINT blocks_number_unique UNIQUE (number)
);

CREATE INDEX IF NOT EXISTS idx_blocks_number_desc ON chain.blocks(number DESC);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON chain.blocks(timestamp);
CREATE INDEX IF NOT EXISTS idx_blocks_number ON chain.blocks(number);

CREATE TABLE IF NOT EXISTS chain.transactions (
    hash            VARCHAR(66) PRIMARY KEY,
    block_hash      VARCHAR(66),
    block_number    BIGINT,
    from_address    VARCHAR(50),
    to_address      VARCHAR(50),
    value           NUMERIC(78, 0),
    nonce           BIGINT,
    gas_limit       BIGINT,
    gas_price       NUMERIC(78, 0),
    gas_fee_cap     NUMERIC(78, 0),
    gas_tip_cap     NUMERIC(78, 0),
    data            TEXT,
    data_method     VARCHAR(10),
    type            SMALLINT,
    chain_id        VARCHAR(50),
    status          VARCHAR(10) CHECK (status IN ('committed','pending', 'queued', 'success', 'failed')),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP,
    block_timestamp BIGINT DEFAULT 0,
    CONSTRAINT transactions_hash_unique UNIQUE (hash)
);

CREATE INDEX IF NOT EXISTS idx_transactions_block_hash ON chain.transactions(block_hash);
CREATE INDEX IF NOT EXISTS idx_transactions_block_number ON chain.transactions(block_number);
CREATE INDEX IF NOT EXISTS idx_transactions_from_address ON chain.transactions(from_address);
CREATE INDEX IF NOT EXISTS idx_transactions_to_address ON chain.transactions(to_address);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON chain.transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_sort ON chain.transactions (block_number DESC, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_hash_lookup ON chain.transactions USING HASH (hash);

CREATE TABLE IF NOT EXISTS chain.transaction_logs (
    tx_hash      VARCHAR(66) NOT NULL REFERENCES chain.transactions(hash),
    log_index    INT NOT NULL,
    block_number BIGINT NOT NULL,
    address      VARCHAR(50) NOT NULL,
    topics       TEXT[] NOT NULL,
    data         TEXT NOT NULL,
    PRIMARY KEY (tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_transaction_logs_address_block ON chain.transaction_logs (address, block_number);

CREATE TABLE IF NOT EXISTS chain.metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chain.erc20_watchlist (
    address     VARCHAR(42) PRIMARY KEY,
    symbol      VARCHAR(32),
    decimals    SMALLINT,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    is_private     BOOLEAN NOT NULL DEFAULT FALSE,
    next_block BIGINT NOT NULL DEFAULT 1,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chain.erc20_hourly_stats (
    token_address VARCHAR(42) NOT NULL,
    hour_utc TIMESTAMPTZ NOT NULL,
    transfer_count BIGINT NOT NULL DEFAULT 0,
    transfer_volume_raw NUMERIC(78, 0) NOT NULL DEFAULT 0,
    mint_count BIGINT NOT NULL DEFAULT 0,
    mint_volume_raw NUMERIC(78, 0) NOT NULL DEFAULT 0,
    burn_count BIGINT NOT NULL DEFAULT 0,
    burn_volume_raw NUMERIC(78, 0) NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cumulative_circulation NUMERIC NOT NULL DEFAULT 0,
    PRIMARY KEY (token_address, hour_utc)
);

CREATE INDEX IF NOT EXISTS idx_erc20_hourly_stats_hour ON chain.erc20_hourly_stats(hour_utc DESC);
CREATE INDEX IF NOT EXISTS idx_erc20_hourly_stats_token ON chain.erc20_hourly_stats(token_address);

-- Adoption analytics (syncer --entity-stats): unique EOA addresses per UTC hour (from/to; contracts excluded via eth_getCode).
CREATE TABLE IF NOT EXISTS chain.entity_hour_participation (
    hour_utc TIMESTAMPTZ NOT NULL,
    address VARCHAR(42) NOT NULL,
    PRIMARY KEY (hour_utc, address)
);

CREATE INDEX IF NOT EXISTS idx_entity_hour_participation_hour ON chain.entity_hour_participation(hour_utc DESC);
