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
	// t.Helper()

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

// StartForTestMain starts DB without *testing.T (for TestMain)
func (d *DB) StartForTestMain() {
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
		fmt.Printf("failed to start db: %v\n", err)
		os.Exit(1)
	}

	d.started = true

	// wait for ready without *testing.T
	deadline := time.Now().UTC().Add(30 * time.Second)
	for time.Now().UTC().Before(deadline) {
		cmd := exec.Command("pg_isready",
			"-h", d.config.Host,
			"-p", d.config.Port,
			"-U", d.config.User,
		)
		if cmd.Run() == nil {
			fmt.Println("db ready")

			conn, err := sql.Open("postgres", d.config.ConnString())
			if err != nil {
				fmt.Printf("failed to connect to db: %v\n", err)
				os.Exit(1)
			}

			if err := conn.Ping(); err != nil {
				fmt.Printf("failed to ping db: %v\n", err)
				os.Exit(1)
			}

			d.conn = conn

			return
		}

		time.Sleep(time.Second)
	}

	fmt.Println("db not ready after 30s")
	os.Exit(1)
}

func (d *DB) TruncateAll() {
	_, err := d.conn.Exec(`
		TRUNCATE
			chain.blocks,
			chain.transactions,
			chain.transaction_logs,
			chain.metadata,
			chain.erc20_watchlist,
			chain.erc20_hourly_stats,
			chain.entity_hour_participation,
			chain.validator_metadata,
			chain.asset_issuers,
			chain.asset_issuer_tokens
		CASCADE
	`)
	if err != nil {
		if d.t != nil {
			d.t.Fatalf("failed to truncate: %v", err)
		} else {
			fmt.Printf("failed to truncate: %v\n", err)
		}
	}
}

func (d *DB) SetT(t *testing.T) {
	d.t = t
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

func (d *DB) AddERC20ToWatchlist(address string, symbol string, decimals int) {
	_, err := d.conn.Exec(`
		INSERT INTO chain.erc20_watchlist (address, symbol, decimals)
		VALUES ($1, $2, $3)
		ON CONFLICT (address) DO UPDATE SET enabled = true
	`, address, symbol, decimals)
	if err != nil {
		d.t.Fatalf("failed to add token to watchlist: %s", err)
	}
}

func (d *DB) RemoveERC20FromWatchlist(address common.Address) {
	_, err := d.conn.Exec(`
		UPDATE chain.erc20_watchlist SET enabled = false WHERE address = $1
	`, address.Hex())
	if err != nil {
		d.t.Fatalf("failed to remove token from watchlist: %s", err)
	}
}

func (d *DB) GetBlockTimestamp(ctx context.Context, t *testing.T, blockNumber uint64) uint64 {
	t.Helper()

	var timestamp uint64

	err := d.conn.QueryRowContext(ctx,
		`SELECT timestamp FROM chain.blocks WHERE number = $1`,
		blockNumber,
	).Scan(&timestamp)
	if err != nil {
		t.Fatalf("failed to get timestamp for block %d: %v", blockNumber, err)
	}

	return timestamp
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
	ctx context.Context) TokenHourlyMap {
	query := `
		SELECT 
			token_address, hour_utc, transfer_count, transfer_volume_raw, 
			mint_count, mint_volume_raw, burn_count, burn_volume_raw, 
			cumulative_circulation 
		FROM chain.erc20_hourly_stats
	`

	rows, err := d.conn.QueryContext(ctx, query)
	if err != nil {
		d.t.Fatalf("failed to query: %s", err)
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
			d.t.Fatalf("failed to scan row: %s", err.Error())
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
		d.t.Fatalf("failed to scan rows: %s", err.Error())
	}

	return retMap
}

// [eoa_address] -> [list of timestamps when the given eoa address was active]
type EOAActivityMap map[string][]hexutil.Uint64

func (d *DB) GetEOAParticipationStats(
	ctx context.Context) EOAActivityMap {
	query := `
		SELECT hour_utc, address 
		FROM chain.entity_hour_participation
	`

	rows, err := d.conn.QueryContext(ctx, query)
	if err != nil {
		d.t.Fatalf("failed to query: %s", err)
	}

	defer rows.Close() //nolint:errcheck

	retMap := EOAActivityMap{}

	for rows.Next() {
		var hourUtc time.Time

		var address string

		err := rows.Scan(&hourUtc, &address)
		if err != nil {
			d.t.Fatalf("failed to scan row: %s", err.Error())
		}

		hourTimestamp := hexutil.Uint64(hourUtc.Unix())

		retMap[address] = append(retMap[address], hourTimestamp)
	}

	if err = rows.Err(); err != nil {
		d.t.Fatalf("failed to scan rows: %s", err.Error())
	}

	return retMap
}

func (d *DB) GetERC20NextBlock(address common.Address) uint64 {
	d.t.Helper()

	var nextBlock uint64

	err := d.conn.QueryRowContext(context.TODO(),
		`SELECT next_block FROM chain.erc20_watchlist WHERE address = $1`,
		address.Hex(),
	).Scan(&nextBlock)
	if err != nil {
		d.t.Fatalf("failed to query last block: %s", err)
	}

	return nextBlock
}

func (d *DB) GetLastProcessedBlock() (*uint64, error) {
	d.t.Helper()

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

func (d *DB) GetLastProcessedEOAActivityBlock() (*uint64, error) {
	d.t.Helper()

	var value string

	err := d.conn.QueryRow(`
		SELECT value FROM chain.metadata WHERE key = 'eoa_activity_last_block_processed'
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
	hash string) *types.Transaction {
	d.t.Helper()

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
			d.t.Fatalf("transaction not found in DB for hash: %s", hash)

			return nil
		}

		d.t.Fatalf("failed to query transaction by hash %s: %v", hash, err)

		return nil
	}

	tx.To = toAddress
	tx.BlockHash = blockHash

	if valueStr != nil {
		bi := new(big.Int)
		if _, ok := bi.SetString(*valueStr, 10); !ok {
			d.t.Fatalf("failed to parse value field")
		}

		tx.Value = (*hexutil.Big)(bi)
	}

	if gasPriceStr != nil {
		bi := new(big.Int)
		if _, ok := bi.SetString(*gasPriceStr, 10); !ok {
			d.t.Fatalf("failed to parse gas_price field")
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

func (d *DB) GetLastBlockNumber() uint64 {
	var num uint64

	err := d.conn.QueryRow("SELECT COALESCE(MAX(number), 0) FROM chain.blocks").Scan(&num)
	if err != nil {
		d.t.Fatalf("failed to query last block: %s", err)
	}

	return num
}

func (d *DB) GetTxCountAfterBlock(blockNumber uint64) uint64 {
	var count uint64

	err := d.conn.QueryRow(
		"SELECT COUNT(*) FROM chain.transactions WHERE block_number > $1",
		blockNumber).Scan(&count)
	if err != nil {
		d.t.Fatalf("failed to query tx count: %s", err)
	}

	return count
}

func (d *DB) WaitForBlock(block uint64, timeout time.Duration) error {
	deadline := time.Now().UTC().Add(timeout)
	for time.Now().UTC().Before(deadline) {
		lastBlockPtr, err := d.GetLastProcessedBlock()
		if err != nil {
			return err
		}

		if lastBlockPtr != nil && *lastBlockPtr > block {
			return nil
		}

		time.Sleep(time.Second)
	}

	return fmt.Errorf("timeout: syncer did not process up to block %d within %s", block, timeout)
}

func (d *DB) WaitForERC20Block(address common.Address, maxBlock uint64, timeout time.Duration) error {
	deadline := time.Now().UTC().Add(timeout)
	for time.Now().UTC().Before(deadline) {
		nextBlock := d.GetERC20NextBlock(address)
		if nextBlock > maxBlock {
			return nil
		}

		time.Sleep(time.Second)
	}

	return fmt.Errorf("timeout: erc20 syncer did not process up to block %d within %s", maxBlock, timeout)
}

func (d *DB) GetTotalGasUsed() uint64 {
	var total uint64

	err := d.conn.QueryRow("SELECT COALESCE(SUM(gas_used), 0) FROM chain.blocks WHERE number > 0").Scan(&total)
	if err != nil {
		d.t.Fatalf("failed to query total gas: %s", err)
	}

	return total
}

func (d *DB) GetValidatorStats(validator string) (gasUsed uint64, gasLimit uint64, blockCount int64) {
	err := d.conn.QueryRow(`
		SELECT COALESCE(SUM(gas_used), 0), COALESCE(SUM(gas_limit), 0), COUNT(*)
		FROM chain.blocks WHERE miner = $1 AND number > 0
	`, validator).Scan(&gasUsed, &gasLimit, &blockCount)
	if err != nil {
		d.t.Fatalf("failed to query validator stats: %s", err)
	}

	return
}

func (d *DB) GetBlockMinerAndGas(blockNumber uint64) (miner string, gasUsed uint64) {
	err := d.conn.QueryRow(`
		SELECT miner, gas_used FROM chain.blocks WHERE number = $1
	`, blockNumber).Scan(&miner, &gasUsed)
	if err != nil {
		d.t.Fatalf("failed to query block %d: %s", blockNumber, err)
	}

	return
}

func (d *DB) InsertBlock(block *types.Block) {
	d.t.Helper()

	var baseFee *int64
	if block.BaseFeePerGas != nil {
		v := block.BaseFeePerGas.ToInt().Int64()
		baseFee = &v
	}

	_, err := d.conn.Exec(`
		INSERT INTO chain.blocks (
			hash, number, parent_hash, nonce, sha3_uncles, logs_bloom,
			transactions_root, state_root, receipts_root, miner,
			difficulty, total_difficulty, extra_data, size,
			gas_limit, gas_used, timestamp, mix_hash, base_fee, txn_count
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10,
			$11, $12, $13, $14,
			$15, $16, $17, $18, $19, $20
		)`,
		block.Hash,
		uint64(block.Number),
		block.ParentHash,
		block.Nonce,
		block.Sha3Uncles,
		[]byte(block.LogsBloom),
		block.TransactionsRoot,
		block.StateRoot,
		block.ReceiptsRoot,
		block.Miner,
		uint64(block.Difficulty),
		uint64(block.TotalDifficulty),
		block.ExtraData,
		uint64(block.Size),
		uint64(block.GasLimit),
		uint64(block.GasUsed),
		uint64(block.Timestamp),
		block.MixHash,
		baseFee,
		int64(len(block.Transactions)),
	)
	if err != nil {
		d.t.Fatalf("InsertBlock: failed to insert block %s (number %d): %v",
			block.Hash, uint64(block.Number), err)
	}
}

func (d *DB) InsertBlocks(blocks []*types.Block) {
	d.t.Helper()

	for _, b := range blocks {
		d.InsertBlock(b)
	}
}

func (d *DB) InsertTransaction(tx *types.Transaction) {
	d.t.Helper()

	var blockNumber *uint64
	if tx.BlockNumber != nil {
		v := uint64(*tx.BlockNumber)
		blockNumber = &v
	}

	var blockTimestamp *uint64
	if tx.BlockTimestamp != nil {
		v := uint64(*tx.BlockTimestamp)
		blockTimestamp = &v
	}

	var value *string
	if tx.Value != nil {
		s := tx.Value.ToInt().String()
		value = &s
	}

	var gasPrice *string
	if tx.GasPrice != nil {
		s := tx.GasPrice.ToInt().String()
		gasPrice = &s
	}

	var maxFeePerGas *string
	if tx.MaxFeePerGas != nil {
		s := tx.MaxFeePerGas.ToInt().String()
		maxFeePerGas = &s
	}

	var maxPriorityFeePerGas *string
	if tx.MaxPriorityFeePerGas != nil {
		s := tx.MaxPriorityFeePerGas.ToInt().String()
		maxPriorityFeePerGas = &s
	}

	_, err := d.conn.Exec(`
		INSERT INTO chain.transactions (
			hash, block_hash, block_number, from_address, to_address,
			value, nonce, gas_limit, gas_price, gas_fee_cap, gas_tip_cap,
			data, type, chain_id, status, block_timestamp
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16
		)`,
		tx.Hash,
		tx.BlockHash,
		blockNumber,
		tx.From,
		tx.To,
		value,
		uint64(tx.Nonce),
		uint64(tx.Gas),
		gasPrice,
		maxFeePerGas,
		maxPriorityFeePerGas,
		tx.Input,
		uint64(tx.Type),
		func() *string {
			if tx.ChainID == nil {
				return nil
			}
			s := tx.ChainID.ToInt().String()
			return &s
		}(),
		"success",
		blockTimestamp,
	)
	if err != nil {
		d.t.Fatalf("InsertTransaction: failed to insert tx %s: %v", tx.Hash, err)
	}
}

func (d *DB) InsertTransactions(txs []*types.Transaction) {
	d.t.Helper()

	for _, tx := range txs {
		d.InsertTransaction(tx)
	}
}

func (d *DB) InsertTestBlock(number int, timestamp time.Time, txnCount int) {
	hash := fmt.Sprintf("0x%064x", number)
	parentHash := fmt.Sprintf("0x%064x", number-1)

	_, err := d.conn.Exec(`
		INSERT INTO chain.blocks (hash, number, parent_hash, nonce, sha3_uncles, logs_bloom,
			transactions_root, state_root, receipts_root, miner, difficulty, total_difficulty,
			extra_data, size, gas_limit, gas_used, timestamp, mix_hash, txn_count)
		VALUES ($1, $2, $3, '0x0', '0x0', E'\\x00', '0x0', '0x0', '0x0', '0x0', 0, 0,
			'', 0, 10000000, 0, $4, '0x0', $5)
	`, hash, number, parentHash, timestamp.Unix(), txnCount)
	if err != nil {
		d.t.Fatalf("failed to insert test block %d: %v", number, err)
	}

	for i := 0; i < txnCount; i++ {
		d.InsertTestTransaction(number, i, hash, timestamp)
	}
}

func (d *DB) InsertTestTransaction(blockNumber, index int, blockHash string, timestamp time.Time) {
	txHash := fmt.Sprintf("0x%064d", blockNumber*1000+index)

	_, err := d.conn.Exec(`
		INSERT INTO chain.transactions (hash, block_hash, block_number, from_address, status, block_timestamp)
		VALUES ($1, $2, $3, '0x0000000000000000000000000000000000000000', 'success', $4)
	`, txHash, blockHash, blockNumber, timestamp.Unix())
	if err != nil {
		d.t.Fatalf("failed to insert test tx %d for block %d: %v", index, blockNumber, err)
	}
}

func (d *DB) InsertTestERC20HourlyStat(
	tokenAddress string,
	hourUtc time.Time,
	transferCount int64,
	transferVolume string,
	mintCount int64,
	mintVolume string,
	burnCount int64,
	burnVolume string,
	cumulativeCirculation string,
) {
	_, err := d.conn.Exec(`
		INSERT INTO chain.erc20_hourly_stats (
			token_address, hour_utc,
			transfer_count, transfer_volume_raw,
			mint_count, mint_volume_raw,
			burn_count, burn_volume_raw,
			cumulative_circulation
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, tokenAddress, hourUtc,
		transferCount, transferVolume,
		mintCount, mintVolume,
		burnCount, burnVolume,
		cumulativeCirculation,
	)
	if err != nil {
		d.t.Fatalf("failed to insert test erc20 hourly stat: %v", err)
	}
}
