package erc20_worker

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/common"
)

const emptyBlockSentinel = "notx"

const watchlistReloadStride = 100

// BlockJob is queued after a block’s transactions are committed to Postgres.
type BlockJob struct {
	BlockNumber uint64
	BlockTS     uint64
	Txs         []*types.Transaction
}

type tokenBucket struct {
	transferCount, mintCount, burnCount int64
	transferVol, mintVol, burnVol       *big.Int
}

func newBucket() *tokenBucket {
	return &tokenBucket{
		transferVol: big.NewInt(0),
		mintVol:     big.NewInt(0),
		burnVol:     big.NewInt(0),
	}
}

type processorState struct {
	mu              sync.Mutex
	watch           map[string]struct{}
	lastReloadEpoch uint64
}

// proc caches the watchlist per epoch (block / watchlistReloadStride). It is package-global so
// all ProcessBlock callers share one cache; tests should call [resetWatchlistCache] when needed.
var proc processorState

// resetWatchlistCache clears the in-memory watchlist cache (integration tests; dev tooling).
func resetWatchlistCache() {
	proc.mu.Lock()
	defer proc.mu.Unlock()
	proc.watch = nil
	proc.lastReloadEpoch = 0
}

func normalizeAddr(a string) string {
	if !common.IsHexAddress(a) {
		return ""
	}
	return strings.ToLower(common.HexToAddress(a).Hex())
}

func reloadWatchlist(ctx context.Context, db *sql.DB) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT lower(address) FROM chain.erc20_watchlist WHERE enabled = true
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]struct{})
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, err
		}
		if addr != "" {
			out[addr] = struct{}{}
		}
	}
	return out, rows.Err()
}

func ensureWatchlist(ctx context.Context, db *sql.DB, blockNumber uint64) (map[string]struct{}, error) {
	epoch := blockNumber / watchlistReloadStride

	proc.mu.Lock()
	defer proc.mu.Unlock()

	if proc.watch != nil && epoch == proc.lastReloadEpoch {
		return proc.watch, nil
	}

	w, err := reloadWatchlist(ctx, db)
	if err != nil {
		return nil, err
	}
	proc.watch = w
	proc.lastReloadEpoch = epoch
	return proc.watch, nil
}

// aggregateBlockLogs sums Transfer events per watchlisted token for one block (no I/O).
func aggregateBlockLogs(job BlockJob, watch map[string]struct{}) map[string]*tokenBucket {
	byToken := make(map[string]*tokenBucket)

	for _, tx := range job.Txs {
		if tx == nil || tx.Hash == emptyBlockSentinel {
			continue
		}
		for _, lg := range tx.Logs {
			token, from, to, value, ok := DecodeTransferLog(lg.Address, lg.Topics, lg.Data)
			if !ok {
				continue
			}
			tok := normalizeAddr(token.Hex())
			if tok == "" {
				continue
			}
			if _, in := watch[tok]; !in {
				continue
			}
			class := ClassifyTransfer(from, to)

			b := byToken[tok]
			if b == nil {
				b = newBucket()
				byToken[tok] = b
			}
			switch class {
			case "mint":
				b.mintCount++
				b.mintVol.Add(b.mintVol, value)
			case "burn":
				b.burnCount++
				b.burnVol.Add(b.burnVol, value)
			default:
				b.transferCount++
				b.transferVol.Add(b.transferVol, value)
			}
		}
	}

	return byToken
}

// ProcessBlock decodes Transfer logs for watchlisted tokens and upserts UTC-hour aggregates.
func ProcessBlock(ctx context.Context, db *sql.DB, job BlockJob) error {
	watch, err := ensureWatchlist(ctx, db, job.BlockNumber)
	if err != nil {
		return fmt.Errorf("erc20 watchlist: %w", err)
	}
	if len(watch) == 0 {
		return nil
	}

	byToken := aggregateBlockLogs(job, watch)
	if len(byToken) == 0 {
		return nil
	}

	hourUTC := time.Unix(int64(job.BlockTS), 0).UTC().Truncate(time.Hour)

	for token, b := range byToken {
		if err := upsertHourly(ctx, db, token, hourUTC, b); err != nil {
			return err
		}
	}
	return nil
}

func upsertHourly(ctx context.Context, db *sql.DB, tokenLower string, hourUTC time.Time, b *tokenBucket) error {
	_, err := db.ExecContext(ctx, `
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
		tokenLower,
		hourUTC.UTC(),
		b.transferCount, b.transferVol.String(),
		b.mintCount, b.mintVol.String(),
		b.burnCount, b.burnVol.String(),
	)
	return err
}
