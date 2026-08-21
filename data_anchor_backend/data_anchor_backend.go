package dataanchorbackend

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/helper"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/lib/pq"
)

type PgDataAnchorBackend struct {
	db     *sql.DB
	logger *slog.Logger
}

func NewPgDataAnchorBackend(db *sql.DB, logger ...*slog.Logger) *PgDataAnchorBackend {
	backend := &PgDataAnchorBackend{db: db, logger: slog.Default()}
	if len(logger) > 0 && logger[0] != nil {
		backend.logger = logger[0]
	}

	return backend
}

func (b *PgDataAnchorBackend) GetWatchlist() ([]*types.DataAnchorFactory, error) {
	rows, err := b.db.Query(`
		SELECT factory_address, start_block, next_block, enabled, created_at, updated_at
		FROM chain.data_anchor_factory_watchlist
		ORDER BY factory_address
	`)
	if err != nil {
		return nil, fmt.Errorf("query data-anchor factory watchlist: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	factories := make([]*types.DataAnchorFactory, 0)

	for rows.Next() {
		factory := &types.DataAnchorFactory{}

		if err := rows.Scan(
			&factory.Address,
			&factory.StartBlock,
			&factory.NextBlock,
			&factory.Enabled,
			&factory.CreatedAt,
			&factory.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan data-anchor factory: %w", err)
		}

		factories = append(factories, factory)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate data-anchor factory watchlist: %w", err)
	}

	return factories, nil
}

func (b *PgDataAnchorBackend) GetTip() (*uint64, error) {
	var value string

	err := b.db.QueryRow(`
		SELECT value FROM chain.metadata WHERE key = 'txworker_last_block_processed'
	`).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("query indexed chain tip: %w", err)
	}

	tip, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse indexed chain tip: %w", err)
	}

	return &tip, nil
}

// GetLogs returns only the two event signatures relevant to data-anchor
// processing. Emitter validation is deliberately deferred to ProcessBlock so a
// child discovered in this same block can be recognized before commitments are
// counted.
func (b *PgDataAnchorBackend) GetLogs(blockNumber uint64) ([]types.ReceiptLog, error) {
	rows, err := b.db.Query(`
		SELECT tx_hash, log_index, block_number, address, topics, data
		FROM chain.transaction_logs
		WHERE block_number = $1
			AND topics && $2
		ORDER BY log_index, tx_hash
	`, blockNumber, pq.Array([]string{
		helper.DailyDeployedTopic.Hex(),
		helper.CommittedTopic.Hex(),
	}))
	if err != nil {
		return nil, fmt.Errorf("query data-anchor logs for block %d: %w", blockNumber, err)
	}
	defer rows.Close() //nolint:errcheck

	logs := make([]types.ReceiptLog, 0)

	for rows.Next() {
		var (
			entry  types.ReceiptLog
			topics pq.StringArray
		)

		if err := rows.Scan(
			&entry.TransactionHash,
			&entry.Index,
			&entry.BlockNumber,
			&entry.Address,
			&topics,
			&entry.Data,
		); err != nil {
			return nil, fmt.Errorf("scan data-anchor log: %w", err)
		}

		entry.Topics = []string(topics)
		logs = append(logs, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate data-anchor logs: %w", err)
	}

	if len(logs) > 0 {
		b.log("data-anchor GetLogs block=%d matched=%d (DailyDeployed/Committed topics)",
			blockNumber, len(logs))
	}

	return logs, nil
}

func (b *PgDataAnchorBackend) ProcessBlock(
	blockNumber uint64,
	factory *types.DataAnchorFactory,
	logs []types.ReceiptLog,
) error {
	if factory == nil || !common.IsHexAddress(factory.Address) {
		return fmt.Errorf("invalid data-anchor factory")
	}

	tx, err := b.db.Begin()
	if err != nil {
		return fmt.Errorf("begin data-anchor block transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	children, err := loadDailyContracts(tx, factory.Address)
	if err != nil {
		return err
	}

	if len(logs) > 0 {
		b.log("data-anchor ProcessBlock block=%d factory=%s candidate_logs=%d known_dailies=%d next_block=%d",
			blockNumber, factory.Address, len(logs), len(children), factory.NextBlock)
	}

	factoryAddress := common.HexToAddress(factory.Address)
	discovered := 0

	for i := range logs {
		entry := &logs[i]
		if !hasFirstTopic(entry, helper.DailyDeployedTopic) {
			continue
		}

		if common.HexToAddress(entry.Address) != factoryAddress {
			b.log("data-anchor skip DailyDeployed %s:%d: emitter %s != factory %s",
				entry.TransactionHash, uint64(entry.Index), entry.Address, factory.Address)

			continue
		}

		event, ok := helper.DecodeDailyDeployedLog(entry, factoryAddress)
		if !ok {
			b.log("data-anchor skip DailyDeployed %s:%d factory=%s: %s (topics=%d data_len=%d)",
				entry.TransactionHash,
				uint64(entry.Index),
				factory.Address,
				helper.DailyDeployedRejectReason(entry, factoryAddress),
				len(entry.Topics),
				len(entry.Data),
			)

			continue
		}

		result, err := tx.Exec(`
			INSERT INTO chain.daily_commitment_stats (
				factory_address, day_timestamp, data_type, institution_id,
				daily_contract_address, discovery_block
			)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (daily_contract_address) DO UPDATE SET
				updated_at = chain.daily_commitment_stats.updated_at
			WHERE chain.daily_commitment_stats.factory_address = EXCLUDED.factory_address
				AND chain.daily_commitment_stats.day_timestamp = EXCLUDED.day_timestamp
				AND chain.daily_commitment_stats.data_type = EXCLUDED.data_type
				AND chain.daily_commitment_stats.institution_id = EXCLUDED.institution_id
				AND chain.daily_commitment_stats.discovery_block = EXCLUDED.discovery_block
		`,
			factory.Address,
			event.DayTimestamp,
			event.DataType.Hex(),
			event.InstitutionID.Hex(),
			event.DailyContractAddress.Hex(),
			blockNumber,
		)
		if err != nil {
			return fmt.Errorf("persist DailyDeployed log %s:%d: %w",
				entry.TransactionHash, uint64(entry.Index), err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read DailyDeployed persistence result: %w", err)
		}

		if affected != 1 {
			return fmt.Errorf("DailyDeployed log conflicts with stored daily contract %s",
				event.DailyContractAddress.Hex())
		}

		children[event.DailyContractAddress] = struct{}{}
		discovered++

		b.log("data-anchor discovered daily=%s day=%d institution=%s data_type=%s factory=%s block=%d",
			event.DailyContractAddress.Hex(),
			event.DayTimestamp,
			event.InstitutionID.Hex(),
			event.DataType.Hex(),
			factory.Address,
			blockNumber,
		)
	}

	counts := make(map[common.Address]uint64)

	for i := range logs {
		entry := &logs[i]
		if !hasFirstTopic(entry, helper.CommittedTopic) {
			continue
		}

		emitter := common.HexToAddress(entry.Address)
		if _, known := children[emitter]; !known {
			b.log("data-anchor skip Committed %s:%d: daily %s unknown for factory %s "+
				"(DailyDeployed missing or not yet indexed)",
				entry.TransactionHash, uint64(entry.Index), emitter.Hex(), factory.Address)

			continue
		}

		if _, ok := helper.DecodeCommittedLog(entry, emitter); !ok {
			b.log("data-anchor skip Committed %s:%d daily=%s: %s",
				entry.TransactionHash,
				uint64(entry.Index),
				emitter.Hex(),
				helper.CommittedRejectReason(entry, emitter),
			)

			continue
		}

		counts[emitter]++
	}

	committedTotal := uint64(0)

	for address, count := range counts {
		result, err := tx.Exec(`
			UPDATE chain.daily_commitment_stats
			SET commitment_count = commitment_count + $1,
				updated_at = CURRENT_TIMESTAMP
			WHERE daily_contract_address = $2 AND factory_address = $3
		`, count, address.Hex(), factory.Address)
		if err != nil {
			return fmt.Errorf("update commitment count for %s: %w", address.Hex(), err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read commitment update result for %s: %w", address.Hex(), err)
		}

		if affected != 1 {
			return fmt.Errorf("daily contract %s is not owned by factory %s", address.Hex(), factory.Address)
		}

		committedTotal += count
		b.log("data-anchor counted +%d commitments for daily=%s factory=%s block=%d",
			count, address.Hex(), factory.Address, blockNumber)
	}

	result, err := tx.Exec(`
		UPDATE chain.data_anchor_factory_watchlist
		SET next_block = $1, updated_at = CURRENT_TIMESTAMP
		WHERE factory_address = $2 AND next_block = $3 AND enabled = TRUE
	`, blockNumber+1, factory.Address, blockNumber)
	if err != nil {
		return fmt.Errorf("advance data-anchor factory cursor: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read data-anchor cursor update result: %w", err)
	}

	if affected != 1 {
		return fmt.Errorf("%w: factory %s is not at block %d",
			types.ErrDataAnchorCursorChanged, factory.Address, blockNumber)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit data-anchor block transaction: %w", err)
	}

	if discovered > 0 || committedTotal > 0 || len(logs) > 0 {
		b.log("data-anchor ProcessBlock done block=%d factory=%s discovered=%d committed=%d cursor->%d",
			blockNumber, factory.Address, discovered, committedTotal, blockNumber+1)
	}

	return nil
}

func loadDailyContracts(tx *sql.Tx, factoryAddress string) (map[common.Address]struct{}, error) {
	rows, err := tx.Query(`
		SELECT daily_contract_address
		FROM chain.daily_commitment_stats
		WHERE factory_address = $1
	`, factoryAddress)
	if err != nil {
		return nil, fmt.Errorf("query known daily contracts: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	children := make(map[common.Address]struct{})

	for rows.Next() {
		var address string

		if err := rows.Scan(&address); err != nil {
			return nil, fmt.Errorf("scan known daily contract: %w", err)
		}

		if !common.IsHexAddress(address) {
			return nil, fmt.Errorf("stored daily contract has invalid address %q", address)
		}

		children[common.HexToAddress(address)] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate known daily contracts: %w", err)
	}

	return children, nil
}

func hasFirstTopic(log *types.ReceiptLog, topic common.Hash) bool {
	// Compare via HexToHash so mixed-case topic strings from Postgres still match.
	return log != nil && len(log.Topics) > 0 && common.HexToHash(log.Topics[0]) == topic
}

func (b *PgDataAnchorBackend) log(format string, args ...any) {
	logger := b.logger
	if logger == nil {
		logger = slog.Default()
	}

	logger.Info(fmt.Sprintf(format, args...), "component", "data_anchor_backend")
}
