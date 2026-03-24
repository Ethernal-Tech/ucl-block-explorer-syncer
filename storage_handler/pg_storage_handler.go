package storage_handler

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/common/hexutil"

	_ "github.com/lib/pq"
)

type PgStorageHandler struct {
	db      *sql.DB
	withTxs bool
}

func NewPgStorageHandler(connString string, withTxs bool) (*PgStorageHandler, error) {
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, fmt.Errorf("cannot open postgres db: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("db ping error: %w", err)
	}

	return &PgStorageHandler{db, withTxs}, nil
}

func (h *PgStorageHandler) Shutdown() error {
	if err := h.db.Close(); err != nil {
		return err
	}

	return nil
}

func (h *PgStorageHandler) InsertBlock(block *types.Block) error {
	tx, err := h.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback()

	var baseFee *uint64
	if block.BaseFeePerGas != nil {
		fee := block.BaseFeePerGas.ToInt().Uint64()
		baseFee = &fee
	}

	logsBloom, err := hexutil.Decode(block.LogsBloom)
	if err != nil {
		return fmt.Errorf("failed to decode logsBloom: %w", err)
	}

	if _, err := tx.Exec(`
		INSERT INTO chain.blocks (
			hash, number, parent_hash, nonce, sha3_uncles, logs_bloom,
			transactions_root, state_root, receipts_root, miner,
			difficulty, total_difficulty, extra_data, size,
			gas_limit, gas_used, timestamp, mix_hash, base_fee, txn_count
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, $18, $19, $20
		)
		ON CONFLICT DO NOTHING
	`,
		block.Hash,
		uint64(block.Number),
		block.ParentHash,
		block.Nonce,
		block.Sha3Uncles,
		logsBloom,
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
		len(block.Transactions),
	); err != nil {
		return fmt.Errorf("failed to insert block: %w", err)
	}

	if len(block.Transactions) > 0 {
		paramsPerRow := 5
		if h.withTxs {
			paramsPerRow = 17
		}

		chunkSize := 65535 / paramsPerRow

		for i := 0; i < len(block.Transactions); i += chunkSize {
			end := min(i+chunkSize, len(block.Transactions))
			chunk := block.Transactions[i:end]

			if err := h.batchInsertCommittedTransactions(tx, chunk); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (h *PgStorageHandler) InsertTransactions(txs []*types.Transaction) error {
	if txs[0].Hash == "notx" {
		return h.setTxWorkerLastBlockProcessed(nil, uint64(*txs[0].BlockNumber))
	}

	tx, err := h.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback()

	paramsPerRow := 2
	if !h.withTxs {
		paramsPerRow = 14
	}

	chunkSize := 65535 / paramsPerRow

	for i := 0; i < len(txs); i += chunkSize {
		end := min(i+chunkSize, len(txs))
		chunk := txs[i:end]

		if err := h.batchInsertTransactionsWithStatus(tx, chunk); err != nil {
			return err
		}
	}

	if err := h.setTxWorkerLastBlockProcessed(tx, uint64(*txs[0].BlockNumber)); err != nil {
		return err
	}

	return tx.Commit()
}

func (h *PgStorageHandler) setTxWorkerLastBlockProcessed(tx *sql.Tx, blockNumber uint64) error {
	var err error

	query := `
		INSERT INTO chain.metadata (key, value) VALUES ('txworker_last_block_processed', $1)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
	`

	if tx != nil {
		_, err = tx.Exec(query, strconv.FormatUint(blockNumber, 10))
	} else {
		_, err = h.db.Exec(query, strconv.FormatUint(blockNumber, 10))
	}

	if err != nil {
		return fmt.Errorf("failed to set txworker last block processed: %w", err)
	}

	return nil
}

func (h *PgStorageHandler) batchInsertCommittedTransactions(tx *sql.Tx, txs []*types.Transaction) error {
	var (
		placeholders []string
		args         []any
		argIdx       = 1
	)

	bigToStringFn := func(b *hexutil.Big) *string {
		if b == nil {
			return nil
		}

		str := b.ToInt().String()

		return &str
	}

	for _, t := range txs {
		var dataMethod *string

		if len(t.Input) >= 10 && t.To != nil && *t.To != "" {
			m := t.Input[:10]
			dataMethod = &m
		}

		if h.withTxs {
			placeholders = append(placeholders, fmt.Sprintf(
				"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
				argIdx, argIdx+1, argIdx+2, argIdx+3, argIdx+4, argIdx+5,
				argIdx+6, argIdx+7, argIdx+8, argIdx+9, argIdx+10, argIdx+11,
				argIdx+12, argIdx+13, argIdx+14, argIdx+15, argIdx+16,
			))

			args = append(args,
				t.Hash,
				*t.BlockHash,
				uint64(*t.BlockNumber),
				uint64(*t.BlockTimestamp),
				t.From,
				t.To,
				bigToStringFn(t.Value),
				uint64(t.Nonce),
				uint64(t.Gas),
				bigToStringFn(t.GasPrice),
				bigToStringFn(t.MaxFeePerGas),
				bigToStringFn(t.MaxPriorityFeePerGas),
				t.Input,
				dataMethod,
				uint64(t.Type),
				bigToStringFn(t.ChainID),
				"committed",
			)

			argIdx += 17
		} else {
			placeholders = append(placeholders, fmt.Sprintf(
				"($%d,$%d,$%d,$%d,$%d)",
				argIdx, argIdx+1, argIdx+2, argIdx+3, argIdx+4,
			))

			args = append(args,
				t.Hash,
				*t.BlockHash,
				uint64(*t.BlockNumber),
				uint64(*t.BlockTimestamp),
				"committed",
			)

			argIdx += 5
		}
	}

	var query string

	if h.withTxs {
		query = `
			INSERT INTO chain.transactions (
				hash, block_hash, block_number, block_timestamp, from_address, to_address,
				value, nonce, gas_limit, gas_price, gas_fee_cap, gas_tip_cap,
				data, data_method, type, chain_id, status
			) VALUES ` + strings.Join(placeholders, ", ") + `
			ON CONFLICT (hash) DO UPDATE
    		SET
    		    block_hash      = EXCLUDED.block_hash,
    		    block_number    = EXCLUDED.block_number,
    		    block_timestamp = EXCLUDED.block_timestamp,
    		    status          = EXCLUDED.status,
    		    updated_at      = NOW()
		`
	} else {
		query = `
			INSERT INTO chain.transactions (
				hash, block_hash, block_number, block_timestamp, status
			) VALUES ` + strings.Join(placeholders, ", ") + `
			ON CONFLICT (hash) DO UPDATE
    		SET
    		    block_hash      = EXCLUDED.block_hash,
    		    block_number    = EXCLUDED.block_number,
    		    block_timestamp = EXCLUDED.block_timestamp,
    		    status          = EXCLUDED.status,
    		    updated_at      = NOW()
		`
	}

	if _, err := tx.Exec(query, args...); err != nil {
		return fmt.Errorf("failed to batch insert transactions: %w", err)
	}

	return nil
}

func (h *PgStorageHandler) batchInsertTransactionsWithStatus(tx *sql.Tx, txs []*types.Transaction) error {
	var (
		placeholders []string
		args         []any
		argIdx       = 1
	)

	bigToStringFn := func(b *hexutil.Big) *string {
		if b == nil {
			return nil
		}

		str := b.ToInt().String()

		return &str
	}

	statusFn := func(s hexutil.Uint64) string {
		if s == 1 {
			return "success"
		}

		return "failed"
	}

	for _, t := range txs {
		if h.withTxs {
			placeholders = append(placeholders, fmt.Sprintf(
				"($%d,$%d)",
				argIdx, argIdx+1,
			))

			args = append(args,
				t.Hash,
				statusFn(t.Status),
			)

			argIdx += 2
		} else {
			var dataMethod *string

			if len(t.Input) >= 10 && t.To != nil && *t.To != "" {
				m := t.Input[:10]
				dataMethod = &m
			}

			placeholders = append(placeholders, fmt.Sprintf(
				"($%d,$%d,$%d,$%d::NUMERIC,$%d::BIGINT,$%d::BIGINT,$%d::NUMERIC,$%d::NUMERIC,$%d::NUMERIC,$%d,$%d,$%d::SMALLINT,$%d,$%d)",
				argIdx, argIdx+1, argIdx+2, argIdx+3, argIdx+4, argIdx+5,
				argIdx+6, argIdx+7, argIdx+8, argIdx+9, argIdx+10, argIdx+11,
				argIdx+12, argIdx+13,
			))

			args = append(args,
				t.Hash,
				t.From,
				t.To,
				bigToStringFn(t.Value),
				uint64(t.Nonce),
				uint64(t.Gas),
				bigToStringFn(t.GasPrice),
				bigToStringFn(t.MaxFeePerGas),
				bigToStringFn(t.MaxPriorityFeePerGas),
				t.Input,
				dataMethod,
				uint64(t.Type),
				bigToStringFn(t.ChainID),
				statusFn(t.Status),
			)

			argIdx += 14
		}
	}

	var query string

	if h.withTxs {
		query = `
			UPDATE chain.transactions SET
				status     = v.status,
				updated_at = NOW()
			FROM (VALUES ` + strings.Join(placeholders, ", ") + `) AS v(hash, status)
			WHERE chain.transactions.hash = v.hash
		`
	} else {
		query = `
			UPDATE chain.transactions SET
				from_address = v.from_address,
				to_address   = v.to_address,
				value        = v.value,
				nonce        = v.nonce,
				gas_limit    = v.gas_limit,
				gas_price    = v.gas_price,
				gas_fee_cap  = v.gas_fee_cap,
				gas_tip_cap  = v.gas_tip_cap,
				data         = v.data,
				data_method  = v.data_method,
				type         = v.type,
				chain_id     = v.chain_id,
				status       = v.status,
				updated_at   = NOW()
			FROM (VALUES ` + strings.Join(placeholders, ", ") + `) AS 
			v(hash, 
			from_address, 
			to_address, 
			value, 
			nonce, 
			gas_limit, 
			gas_price, 
			gas_fee_cap, 
			gas_tip_cap, 
			data, 
			data_method,
			type, 
			chain_id, 
			status)
			WHERE chain.transactions.hash = v.hash
		`
	}

	if _, err := tx.Exec(query, args...); err != nil {
		return fmt.Errorf("failed to batch update transactions: %w", err)
	}

	return nil
}

func (h *PgStorageHandler) ShouldFetchFullTransaction(string) bool {
	return !h.withTxs
}

func (h *PgStorageHandler) GetBlock(number uint64) (*types.Block, error) {
	var block types.Block

	row := h.db.QueryRow(`
		SELECT hash, number, timestamp
		FROM chain.blocks
		WHERE number = $1
	`, number)

	var num uint64
	var timestamp uint64

	if err := row.Scan(&block.Hash, &num, &timestamp); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("block %d not found", number)
		}

		return nil, fmt.Errorf("failed to query block: %w", err)
	}

	block.Number = hexutil.Uint64(num)
	block.Timestamp = hexutil.Uint64(timestamp)

	rows, err := h.db.Query(`
		SELECT hash, block_hash, block_number, block_timestamp
		FROM chain.transactions
		WHERE block_number = $1
	`, number)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		var tx types.Transaction
		var blockHash string
		var blockNumber uint64
		var blockTimestamp uint64

		if err := rows.Scan(&tx.Hash, &blockHash, &blockNumber, &blockTimestamp); err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		}

		tx.BlockHash = &blockHash
		bn := hexutil.Uint64(blockNumber)
		tx.BlockNumber = &bn
		bt := hexutil.Uint64(blockTimestamp)
		tx.BlockTimestamp = &bt

		block.Transactions = append(block.Transactions, &tx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate transactions: %w", err)
	}

	return &block, nil
}

func (h *PgStorageHandler) InsertPoolTransactions(pending, queued []*types.Transaction) error {
	if len(pending)+len(queued) == 0 {
		return nil
	}

	db, err := h.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer db.Rollback()

	paramsPerRow := 14
	chunkSize := 65535 / paramsPerRow

	if err := h.batchInsertPoolTransactions(db, queued, "queued", chunkSize); err != nil {
		return err
	}

	if err := h.batchInsertPoolTransactions(db, pending, "pending", chunkSize); err != nil {
		return err
	}

	return db.Commit()
}

func (h *PgStorageHandler) batchInsertPoolTransactions(db *sql.Tx, txs []*types.Transaction, status string, chunkSize int) error {
	for i := 0; i < len(txs); i += chunkSize {
		end := min(i+chunkSize, len(txs))

		if err := h.batchInsertPoolTransactionsChunk(db, txs[i:end], status); err != nil {
			return err
		}
	}

	return nil
}

func (h *PgStorageHandler) batchInsertPoolTransactionsChunk(db *sql.Tx, txs []*types.Transaction, status string) error {
	var (
		placeholders []string
		args         []any
		argIdx       = 1
	)

	bigToStringFn := func(b *hexutil.Big) *string {
		if b == nil {
			return nil
		}

		str := b.ToInt().String()

		return &str
	}

	for _, t := range txs {
		var dataMethod *string

		if len(t.Input) >= 10 && t.To != nil {
			m := t.Input[:10]
			dataMethod = &m
		}

		placeholders = append(placeholders, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			argIdx, argIdx+1, argIdx+2, argIdx+3, argIdx+4, argIdx+5,
			argIdx+6, argIdx+7, argIdx+8, argIdx+9, argIdx+10, argIdx+11,
			argIdx+12, argIdx+13,
		))

		args = append(args,
			t.Hash,
			t.From,
			t.To,
			bigToStringFn(t.Value),
			uint64(t.Nonce),
			uint64(t.Gas),
			bigToStringFn(t.GasPrice),
			bigToStringFn(t.MaxFeePerGas),
			bigToStringFn(t.MaxPriorityFeePerGas),
			t.Input,
			dataMethod,
			uint64(t.Type),
			bigToStringFn(t.ChainID),
			status,
		)

		argIdx += 14
	}

	query := `
		INSERT INTO chain.transactions (
		    hash, from_address, to_address,
		    value, nonce, gas_limit, gas_price, gas_fee_cap, gas_tip_cap,
		    data, data_method, type, chain_id, status
		)
		VALUES ` + strings.Join(placeholders, ", ") + `
		ON CONFLICT (hash) DO UPDATE
			SET
				status     = EXCLUDED.status,
				updated_at = NOW()
			WHERE chain.transactions.status IN ('pending', 'queued')
			  AND chain.transactions.status IS DISTINCT FROM EXCLUDED.status;
	`

	if _, err := db.Exec(query, args...); err != nil {
		return fmt.Errorf("failed to batch insert pool transactions: %w", err)
	}

	return nil
}

func (h *PgStorageHandler) GetLastBlockNumber() (*uint64, error) {
	var number *uint64

	err := h.db.QueryRow(`SELECT MAX(number) FROM chain.blocks`).Scan(&number)

	if err != nil {
		return nil, fmt.Errorf("failed to query last block number: %w", err)
	}

	return number, nil
}

func (h *PgStorageHandler) GetTxWorkerLastBlockProcessed() (*uint64, error) {
	var value string

	err := h.db.QueryRow(`
		SELECT value FROM chain.metadata WHERE key = 'txworker_last_block_processed'
	`).Scan(&value)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to query txworker last block processed: %w", err)
	}

	number, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse txworker last block processed: %w", err)
	}

	return &number, nil
}
