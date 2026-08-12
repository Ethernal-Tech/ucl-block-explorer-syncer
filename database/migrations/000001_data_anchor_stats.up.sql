CREATE SCHEMA IF NOT EXISTS chain;

CREATE TABLE IF NOT EXISTS chain.data_anchor_factory_watchlist (
    factory_address VARCHAR(42) PRIMARY KEY,
    start_block BIGINT NOT NULL CHECK (start_block >= 0),
    next_block BIGINT NOT NULL CHECK (next_block >= 0),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (next_block >= start_block)
);

CREATE TABLE IF NOT EXISTS chain.daily_commitment_stats (
    factory_address VARCHAR(42) NOT NULL
        REFERENCES chain.data_anchor_factory_watchlist(factory_address),
    day_timestamp BIGINT NOT NULL CHECK (day_timestamp > 0 AND day_timestamp % 86400 = 0),
    data_type VARCHAR(66) NOT NULL,
    institution_id VARCHAR(66) NOT NULL,
    daily_contract_address VARCHAR(42) PRIMARY KEY,
    commitment_count BIGINT NOT NULL DEFAULT 0 CHECK (commitment_count >= 0),
    discovery_block BIGINT NOT NULL CHECK (discovery_block >= 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT daily_commitment_stats_scope_unique
        UNIQUE (factory_address, day_timestamp, institution_id, data_type)
);

CREATE INDEX IF NOT EXISTS idx_daily_commitment_stats_day
    ON chain.daily_commitment_stats(day_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_daily_commitment_stats_factory_day
    ON chain.daily_commitment_stats(factory_address, day_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_daily_commitment_stats_institution
    ON chain.daily_commitment_stats(institution_id);
CREATE INDEX IF NOT EXISTS idx_daily_commitment_stats_data_type
    ON chain.daily_commitment_stats(data_type);
