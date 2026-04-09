package entity_stats_worker

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

const emptyBlockSentinel = "notx"

// BlockJob is queued after each block’s transactions are committed (same shape as ERC-20 worker).
type BlockJob struct {
	BlockNumber uint64
	BlockTS     uint64
	Txs         []*types.Transaction
}

func normalizeAddr(a string) string {
	a = strings.TrimSpace(a)
	if !common.IsHexAddress(a) {
		return ""
	}
	h := common.HexToAddress(a)
	if h == (common.Address{}) {
		return ""
	}
	return strings.ToLower(h.Hex())
}

func insertParticipationBatch(ctx context.Context, db *sql.DB, hourUTC time.Time, addrs []string) error {
	if len(addrs) == 0 {
		return nil
	}
	const chunk = 80
	for i := 0; i < len(addrs); i += chunk {
		j := i + chunk
		if j > len(addrs) {
			j = len(addrs)
		}
		var sb strings.Builder
		sb.WriteString(`INSERT INTO chain.entity_hour_participation (hour_utc, address) VALUES `)
		args := make([]any, 0, (j-i)*2)
		p := 1
		for k := i; k < j; k++ {
			if k > i {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("($%d::timestamptz, $%d)", p, p+1))
			args = append(args, hourUTC.UTC(), addrs[k])
			p += 2
		}
		sb.WriteString(` ON CONFLICT DO NOTHING`)
		if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("entity participation batch: %w", err)
		}
	}
	return nil
}

// isEOAAtBlock returns true if addr has no contract code at blockNum (eth_getCode empty).
func isEOAAtBlock(ctx context.Context, ec *ethclient.Client, addr string, blockNum uint64) (bool, error) {
	code, err := ec.CodeAt(ctx, common.HexToAddress(addr), big.NewInt(int64(blockNum)))
	if err != nil {
		return false, fmt.Errorf("eth_getCode %s@%d: %w", addr, blockNum, err)
	}
	return len(code) == 0, nil
}

// upsertEOAFirstSeenHour records or advances first_seen_hour_utc for an address already known to be EOA.
func upsertEOAFirstSeenHour(ctx context.Context, db *sql.DB, addr string, hourUTC time.Time) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO chain.eoa_first_seen (address, first_seen_hour_utc) VALUES ($1, $2::timestamptz)
		ON CONFLICT (address) DO UPDATE SET
			first_seen_hour_utc = LEAST(chain.eoa_first_seen.first_seen_hour_utc, EXCLUDED.first_seen_hour_utc),
			updated_at = CURRENT_TIMESTAMP
	`, addr, hourUTC.UTC())
	return err
}

// ProcessBlock records per-hour unique EOA participants (contracts excluded via eth_getCode) and first-seen EOA hours.
func ProcessBlock(ctx context.Context, db *sql.DB, ec *ethclient.Client, job BlockJob) error {
	if ec == nil {
		return fmt.Errorf("eth client is nil")
	}
	hourUTC := time.Unix(int64(job.BlockTS), 0).UTC().Truncate(time.Hour)

	registryOrder := make([]string, 0, 32)
	seenReg := make(map[string]struct{})

	for _, tx := range job.Txs {
		if tx == nil || tx.Hash == emptyBlockSentinel {
			continue
		}
		addrs := []string{normalizeAddr(tx.From)}
		if tx.To != nil {
			addrs = append(addrs, normalizeAddr(*tx.To))
		}
		for _, a := range addrs {
			if a == "" {
				continue
			}
			if _, ok := seenReg[a]; !ok {
				seenReg[a] = struct{}{}
				registryOrder = append(registryOrder, a)
			}
		}
	}

	eoaAddrs := make([]string, 0, len(registryOrder))
	for _, addr := range registryOrder {
		eoa, err := isEOAAtBlock(ctx, ec, addr, job.BlockNumber)
		if err != nil {
			return err
		}
		if !eoa {
			continue
		}
		eoaAddrs = append(eoaAddrs, addr)
	}

	if err := insertParticipationBatch(ctx, db, hourUTC, eoaAddrs); err != nil {
		return err
	}

	for _, addr := range eoaAddrs {
		if err := upsertEOAFirstSeenHour(ctx, db, addr, hourUTC); err != nil {
			return err
		}
	}
	return nil
}
