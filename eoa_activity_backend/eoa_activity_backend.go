package eoaactivitybackend

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/lib/pq"
)

type PgEoaActivityBackend struct {
	db *sql.DB
}

func NewPgEoaActivityBackend(db *sql.DB) *PgEoaActivityBackend {
	return &PgEoaActivityBackend{db: db}
}

func (b *PgEoaActivityBackend) GetLastProcessedBlock() (*uint64, error) {
	var value string

	err := b.db.QueryRow(`
		SELECT value FROM chain.metadata WHERE key = 'eoa_activity_last_block_processed'
	`).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get eoa activity last block processed: %w", err)
	}

	block, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse eoa activity last block processed: %w", err)
	}

	return &block, nil
}

func (b *PgEoaActivityBackend) GetBlockParticipants(blockNum uint64) ([]*types.BlockParticipant, error) {
	var value string

	err := b.db.QueryRow(`
		SELECT value FROM chain.metadata WHERE key = 'txworker_last_block_processed'
	`).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	lastProcessed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	if blockNum > lastProcessed {
		return nil, nil
	}

	rows, err := b.db.Query(`
		SELECT from_address, to_address
		FROM chain.transactions
		WHERE block_number = $1
	`, blockNum)
	if err != nil {
		return nil, fmt.Errorf("failed to query block participants for block %d: %w", blockNum, err)
	}

	defer rows.Close()

	participants := []*types.BlockParticipant{}

	for rows.Next() {
		p := &types.BlockParticipant{}

		if err := rows.Scan(&p.From, &p.To); err != nil {
			return nil, fmt.Errorf("failed to scan block participant: %w", err)
		}

		participants = append(participants, p)
	}

	return participants, nil
}

func (b *PgEoaActivityBackend) FilterKnownEOAs(addresses []string) ([]string, error) {
	if len(addresses) == 0 {
		return nil, nil
	}

	rows, err := b.db.Query(`
		SELECT DISTINCT address FROM chain.entity_hour_participation
		WHERE address = ANY($1)
	`, pq.Array(addresses))
	if err != nil {
		return nil, fmt.Errorf("failed to filter known EOAs: %w", err)
	}

	defer rows.Close()

	var known []string

	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, fmt.Errorf("failed to scan known EOA: %w", err)
		}

		known = append(known, addr)
	}

	return known, nil
}

func (b *PgEoaActivityBackend) RecordEOAActivity(blockNum uint64, addresses []string) error {
	var timestamp int64

	err := b.db.QueryRow(`
		SELECT timestamp FROM chain.blocks WHERE number = $1
	`, blockNum).Scan(&timestamp)
	if err != nil {
		return fmt.Errorf("failed to get block timestamp for block %d: %w", blockNum, err)
	}

	hour := time.Unix(timestamp, 0).UTC().Truncate(time.Hour)

	tx, err := b.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback()

	for _, addr := range addresses {
		_, err := tx.Exec(`
			INSERT INTO chain.entity_hour_participation (hour_utc, address)
			VALUES ($1, $2)
			ON CONFLICT DO NOTHING
		`, hour, addr)

		if err != nil {
			return fmt.Errorf("failed to record EOA activity for address %s: %w", addr, err)
		}
	}

	_, err = tx.Exec(`
		INSERT INTO chain.metadata (key, value) VALUES ('eoa_activity_last_block_processed', $1)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
	`, strconv.FormatUint(blockNum, 10))

	if err != nil {
		return fmt.Errorf("failed to update eoa activity last block processed: %w", err)
	}

	return tx.Commit()
}
