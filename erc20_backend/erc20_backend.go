package erc20backend

import (
	"database/sql"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/lib/pq"
)

type PgErc20Backend struct {
	db *sql.DB
}

func NewPgErc20Backend(db *sql.DB) *PgErc20Backend {
	return &PgErc20Backend{db: db}
}

func (b *PgErc20Backend) GetWatchlist() ([]*types.ERC20Token, error) {
	rows, err := b.db.Query(`
		SELECT address, symbol, decimals, enabled, is_private, next_block
		FROM chain.erc20_watchlist
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query erc20 watchlist: %w", err)
	}

	defer rows.Close()

	var tokens []*types.ERC20Token

	for rows.Next() {
		t := &types.ERC20Token{}

		if err := rows.Scan(
			&t.Address,
			&t.Symbol,
			&t.Decimals,
			&t.Enabled,
			&t.IsPrivate,
			&t.NextBlock,
		); err != nil {
			return nil, fmt.Errorf("failed to scan erc20 token: %w", err)
		}

		tokens = append(tokens, t)
	}

	return tokens, nil
}

func (b *PgErc20Backend) GetTip() (uint64, error) {
	var value string

	err := b.db.QueryRow(`
		SELECT value FROM chain.metadata WHERE key = 'txworker_last_block_processed'
	`).Scan(&value)
	if err == sql.ErrNoRows {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("failed to query tip: %w", err)
	}

	tip, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse tip: %w", err)
	}

	return tip, nil
}

func (b *PgErc20Backend) GetBlock(number uint64) (*types.Block, error) {
	block := &types.Block{}

	err := b.db.QueryRow(`
		SELECT hash, number, timestamp FROM chain.blocks WHERE number = $1
	`, number).Scan(
		&block.Hash,
		&block.Number,
		&block.Timestamp,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("block %d not found", number)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query block %d: %w", number, err)
	}

	return block, nil
}

func (b *PgErc20Backend) GetLogs(blockNum uint64, tokenAddr string, topics []string) ([]types.ReceiptLog, error) {
	rows, err := b.db.Query(`
		SELECT tx_hash, log_index, block_number, address, topics, data
		FROM chain.transaction_logs
		WHERE block_number = $1
		AND address = $2
		AND topics && $3
	`, blockNum, tokenAddr, pq.Array(topics))
	if err != nil {
		return nil, fmt.Errorf("failed to query logs for block %d: %w", blockNum, err)
	}

	defer rows.Close()

	var logs []types.ReceiptLog

	for rows.Next() {
		var l types.ReceiptLog
		var t pq.StringArray

		if err := rows.Scan(
			&l.TransactionHash,
			&l.Index,
			&l.BlockNumber,
			&l.Address,
			&t,
			&l.Data,
		); err != nil {
			return nil, fmt.Errorf("failed to scan log: %w", err)
		}

		l.Topics = []string(t)
		logs = append(logs, l)
	}

	return logs, nil
}

func (b *PgErc20Backend) ProcessHourlyStat(
	blockNum uint64,
	token *types.ERC20Token,
	hour time.Time,
	counts map[string]uint64,
	volumes map[string]*big.Int,
) error {
	tx, err := b.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO chain.erc20_hourly_stats (
			token_address, hour_utc,
			transfer_count, transfer_volume_raw,
			mint_count, mint_volume_raw,
			burn_count, burn_volume_raw
		) VALUES (
			$1, $2::timestamptz,
			$3, $4,
			$5, $6,
			$7, $8
		)
		ON CONFLICT (token_address, hour_utc) DO UPDATE SET
			transfer_count = chain.erc20_hourly_stats.transfer_count + EXCLUDED.transfer_count,
			transfer_volume_raw = chain.erc20_hourly_stats.transfer_volume_raw + EXCLUDED.transfer_volume_raw,
			mint_count = chain.erc20_hourly_stats.mint_count + EXCLUDED.mint_count,
			mint_volume_raw = chain.erc20_hourly_stats.mint_volume_raw + EXCLUDED.mint_volume_raw,
			burn_count = chain.erc20_hourly_stats.burn_count + EXCLUDED.burn_count,
			burn_volume_raw = chain.erc20_hourly_stats.burn_volume_raw + EXCLUDED.burn_volume_raw,
			updated_at = CURRENT_TIMESTAMP
	`,
		token.Address,
		hour.UTC(),
		counts["transfer"], volumes["transfer"].String(),
		counts["mint"], volumes["mint"].String(),
		counts["burn"], volumes["burn"].String(),
	)
	if err != nil {
		return fmt.Errorf("failed to upsert hourly stat for token %s: %w", token.Address, err)
	}

	_, err = tx.Exec(`
		UPDATE chain.erc20_watchlist
		SET next_block = $1, updated_at = CURRENT_TIMESTAMP
		WHERE address = $2
	`, blockNum+1, token.Address)
	if err != nil {
		return fmt.Errorf("failed to update current block for token %s: %w", token.Address, err)
	}

	return tx.Commit()
}
