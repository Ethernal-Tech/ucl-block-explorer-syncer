// e2e/framework/db.go
package framework

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type DB struct {
	conn    *sql.DB
	config  DBConfig
	logsDir string
	started bool
	t       *testing.T
}

func NewDB(t *testing.T, cfg DBConfig, logsDir string) *DB {
	t.Helper()

	return &DB{t: t, config: cfg, logsDir: logsDir}
}

func (d *DB) Start() {
	f, err := os.OpenFile(filepath.Join(d.logsDir, "db.log"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		fmt.Printf("Error creating db log file: %v, falling back to stdout\n", err)

		f = os.Stdout
	}

	cmd := exec.Command("docker", "compose", "up", "-d")
	cmd.Dir = d.config.ComposeDir
	cmd.Stdout = f
	cmd.Stderr = f

	if err := cmd.Run(); err != nil {
		d.t.Fatalf("failed to start db: %v", err)
	}

	d.started = true
	d.waitReady(30 * time.Second)

	conn, err := sql.Open("postgres", d.config.ConnString())
	if err != nil {
		d.t.Fatalf("failed to connect to db: %v", err)
	}

	if err := conn.Ping(); err != nil {
		d.t.Fatalf("failed to ping db: %v", err)
	}

	d.conn = conn
}

func (d *DB) Stop() {
	if d.conn != nil {
		d.conn.Close() //nolint:errcheck

		d.conn = nil
	}

	if !d.started {
		return
	}

	cmd := exec.Command("docker", "compose", "down", "-v")
	cmd.Dir = d.config.ComposeDir

	if err := cmd.Run(); err != nil {
		fmt.Printf("Error executing docker compose down: %v\n", err)
	}

	d.started = false
}

func (d *DB) IsRunning() bool {
	return d.started
}

func (d *DB) Conn() *sql.DB {
	return d.conn
}

func (d *DB) waitReady(timeout time.Duration) {
	deadline := time.Now().UTC().Add(timeout)
	for time.Now().UTC().Before(deadline) {
		cmd := exec.Command("pg_isready", //nolint:gosec
			"-h", d.config.Host,
			"-p", d.config.Port,
			"-U", d.config.User,
		)
		if cmd.Run() == nil {
			d.t.Logf("db ready")

			return
		}

		time.Sleep(time.Second)
	}

	d.t.Fatalf("db not ready after %s", timeout)
}

func (d *DB) GetBlockCount() int {
	var count int

	err := d.conn.QueryRow("SELECT COUNT(*) FROM chain.blocks").Scan(&count)
	if err != nil {
		d.t.Fatalf("failed to query blocks: %s", err)
	}

	return count
}

func (d *DB) AddERC20ToWatchlist(address common.Address) {
	_, err := d.conn.Exec(`
		INSERT INTO chain.erc20_watchlist (address, symbol, decimals)
		VALUES ($1, 'TTK', 18)
		ON CONFLICT (address) DO UPDATE SET enabled = true
	`, address.Hex())
	if err != nil {
		d.t.Fatalf("failed to add token to watchlist: %s", err)
	}
}

type HourlyStats struct {
	TransferCount         int64
	TransferVolumeRaw     *big.Int
	MintCount             int64
	MintVolumeRaw         *big.Int
	BurnCount             int64
	BurnVolumeRaw         *big.Int
	CumulativeCirculation *big.Int
}

// [token] -> [timestamp] -> data
type TokenHourlyMap map[string]map[hexutil.Uint64]HourlyStats

func (d *DB) GetERC20TokensHourlyStatsFromDB(
	ctx context.Context,
	t *testing.T) TokenHourlyMap {
	t.Helper()

	query := `
		SELECT 
			token_address, hour_utc, transfer_count, transfer_volume_raw, 
			mint_count, mint_volume_raw, burn_count, burn_volume_raw, 
			cumulative_circulation 
		FROM chain.erc20_hourly_stats
	`

	rows, err := d.conn.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("failed to query: %s", err)
	}

	defer rows.Close() //nolint:errcheck

	retMap := TokenHourlyMap{}

	for rows.Next() {
		var tokenAddress string

		var hourUtc time.Time

		var transferVolStr, mintVolStr, burnVolStr, cumCircStr string

		var stats HourlyStats

		err := rows.Scan(
			&tokenAddress,
			&hourUtc,
			&stats.TransferCount,
			&transferVolStr,
			&stats.MintCount,
			&mintVolStr,
			&stats.BurnCount,
			&burnVolStr,
			&cumCircStr,
		)
		if err != nil {
			t.Fatalf("failed to scan row: %s", err.Error())
		}

		stats.TransferVolumeRaw = new(big.Int)
		stats.TransferVolumeRaw.SetString(transferVolStr, 10)

		stats.MintVolumeRaw = new(big.Int)
		stats.MintVolumeRaw.SetString(mintVolStr, 10)

		stats.BurnVolumeRaw = new(big.Int)
		stats.BurnVolumeRaw.SetString(burnVolStr, 10)

		stats.CumulativeCirculation = new(big.Int)
		stats.CumulativeCirculation.SetString(cumCircStr, 10)

		hourTimestamp := hexutil.Uint64(hourUtc.Unix())

		if _, exists := retMap[tokenAddress]; !exists {
			retMap[tokenAddress] = make(map[hexutil.Uint64]HourlyStats)
		}

		retMap[tokenAddress][hourTimestamp] = stats
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("failed to scan rows: %s", err.Error())
	}

	return retMap
}

// [eoa_address] -> [list of timestamps when the given eoa address was active]
type EOAActivityMap map[string][]hexutil.Uint64

func (d *DB) GetEOAParticipationStats(
	ctx context.Context,
	t *testing.T) EOAActivityMap {
	t.Helper()

	query := `
		SELECT hour_utc, address 
		FROM chain.entity_hour_participation
	`

	rows, err := d.conn.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("failed to query: %s", err)
	}

	defer rows.Close() //nolint:errcheck

	retMap := EOAActivityMap{}

	for rows.Next() {
		var hourUtc time.Time

		var address string

		err := rows.Scan(&hourUtc, &address)
		if err != nil {
			t.Fatalf("failed to scan row: %s", err.Error())
		}

		hourTimestamp := hexutil.Uint64(hourUtc.Unix())

		retMap[address] = append(retMap[address], hourTimestamp)
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("failed to scan rows: %s", err.Error())
	}

	return retMap
}

func (d *DB) GetLastProcessedBlock(t *testing.T) (*uint64, error) {
	t.Helper()

	var value string
	err := d.conn.QueryRow(`
		SELECT value FROM chain.metadata WHERE key = 'txworker_last_block_processed'
	`).Scan(&value)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to query last block processed: %w", err)
	}

	number, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block number '%s': %w", value, err)
	}

	return &number, nil
}

func (d *DB) GetTransactionByHash(
	ctx context.Context,
	t *testing.T,
	hash string) *types.Transaction {
	t.Helper()

	var tx types.Transaction
	var blockHash, toAddress, data, status *string
	var blockNumber, blockTimestamp *uint64

	var valueStr, gasPriceStr *string

	query := `
		SELECT 
			hash, block_hash, block_number, from_address, to_address, 
			value, nonce, gas_limit, gas_price, data, type, 
			chain_id, status, block_timestamp 
		FROM chain.transactions 
		WHERE hash = $1 
		LIMIT 1
	`

	err := d.conn.QueryRowContext(ctx, query, hash).Scan(
		&tx.Hash,
		&blockHash,
		&blockNumber,
		&tx.From,
		&toAddress,
		&valueStr,
		&tx.Nonce,
		&tx.Gas,
		&gasPriceStr,
		&data,
		&tx.Type,
		&tx.ChainID,
		&status,
		&blockTimestamp,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("transaction not found in DB for hash: %s", hash)
			return nil
		}
		t.Fatalf("failed to query transaction by hash %s: %v", hash, err)
		return nil
	}

	tx.To = toAddress
	tx.BlockHash = blockHash

	if valueStr != nil {
		bi := new(big.Int)
		if _, ok := bi.SetString(*valueStr, 10); !ok {
			t.Fatalf("failed to parse value field")
		}

		tx.Value = (*hexutil.Big)(bi)
	}

	if gasPriceStr != nil {
		bi := new(big.Int)
		if _, ok := bi.SetString(*gasPriceStr, 10); !ok {
			t.Fatalf("failed to parse gas_price field")
		}
		tx.GasPrice = (*hexutil.Big)(bi)
	}

	if data != nil {
		tx.Input = *data
	}

	if status != nil {
		if *status == "success" {
			tx.Status = 1
		} else {
			tx.Status = 0
		}
	}

	if blockNumber != nil {
		hn := hexutil.Uint64(*blockNumber)
		tx.BlockNumber = &hn
	}

	if blockTimestamp != nil {
		ht := hexutil.Uint64(*blockTimestamp)
		tx.BlockTimestamp = &ht
	}

	return &tx
}
