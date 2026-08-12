package e2e

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	syncerdatabase "github.com/Ethernal-Tech/ucl-block-explorer-syncer/database"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
)

func TestIntegration_DataAnchorMigrations(t *testing.T) {
	t.Run("bare database without init snapshot", func(t *testing.T) {
		db := newMigrationTestDatabase(t)

		if err := syncerdatabase.RunMigrations(db, t.Logf); err != nil {
			t.Fatalf("run migration on bare database: %v", err)
		}

		assertDataAnchorMigrationState(t, db)
		assertNoTransactionLogsLookupIndexes(t, db)

		var transactionLogsExists bool
		if err := db.QueryRow(`
			SELECT to_regclass('chain.transaction_logs') IS NOT NULL
		`).Scan(&transactionLogsExists); err != nil {
			t.Fatalf("check transaction_logs on bare database: %v", err)
		}

		if transactionLogsExists {
			t.Fatal("migration unexpectedly created chain.transaction_logs")
		}
	})

	t.Run("populated pre-migration database preserves existing rows", func(t *testing.T) {
		db := newMigrationTestDatabase(t)
		initializeSnapshot(t, db)
		assertNoDataAnchorSchema(t, db)
		assertNoTransactionLogsLookupIndexes(t, db)
		insertLegacyMigrationFixtures(t, db)

		before := legacyMigrationSnapshot(t, db)
		if err := syncerdatabase.RunMigrations(db, t.Logf); err != nil {
			t.Fatalf("run migration: %v", err)
		}

		after := legacyMigrationSnapshot(t, db)

		for table, expected := range before {
			if actual := after[table]; actual != expected {
				t.Fatalf("%s changed during migration:\nbefore=%s\nafter=%s",
					table, expected, actual)
			}
		}

		assertDataAnchorMigrationState(t, db)
		assertNoTransactionLogsLookupIndexes(t, db)
	})

	t.Run("fresh initialized database", func(t *testing.T) {
		db := newMigrationTestDatabase(t)
		initializeSnapshot(t, db)
		assertNoDataAnchorSchema(t, db)
		assertNoTransactionLogsLookupIndexes(t, db)

		if err := syncerdatabase.RunMigrations(db, t.Logf); err != nil {
			t.Fatalf("run migration: %v", err)
		}

		assertDataAnchorMigrationState(t, db)
		assertNoTransactionLogsLookupIndexes(t, db)
	})

	t.Run("repeated starts have no pending migration", func(t *testing.T) {
		db := newMigrationTestDatabase(t)
		initializeSnapshot(t, db)

		for run := 1; run <= 3; run++ {
			if err := syncerdatabase.RunMigrations(db, t.Logf); err != nil {
				t.Fatalf("migration run %d: %v", run, err)
			}
		}

		assertDataAnchorMigrationState(t, db)
	})

	t.Run("concurrent runners serialize", func(t *testing.T) {
		db := newMigrationTestDatabase(t)
		initializeSnapshot(t, db)

		const runners = 4

		errs := make(chan error, runners)

		var wg sync.WaitGroup

		for range runners {
			wg.Add(1)

			go func() {
				defer wg.Done()

				errs <- syncerdatabase.RunMigrations(db, nil)
			}()
		}

		wg.Wait()
		close(errs)

		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent migration runner failed: %v", err)
			}
		}

		assertDataAnchorMigrationState(t, db)
	})

	t.Run("failed migration is fatal and dirty", func(t *testing.T) {
		db := newMigrationTestDatabase(t)
		initializeSnapshot(t, db)
		removeDataAnchorSchema(t, db)

		if _, err := db.Exec(`
			CREATE VIEW chain.data_anchor_factory_watchlist AS
			SELECT
				''::VARCHAR(42) AS factory_address,
				0::BIGINT AS start_block,
				0::BIGINT AS next_block,
				TRUE AS enabled,
				CURRENT_TIMESTAMP AS created_at,
				CURRENT_TIMESTAMP AS updated_at
			WHERE FALSE
		`); err != nil {
			t.Fatalf("create conflicting legacy object: %v", err)
		}

		err := syncerdatabase.RunMigrations(db, nil)
		if err == nil || !strings.Contains(err.Error(), "apply database migrations") {
			t.Fatalf("expected fatal migration failure, got %v", err)
		}

		var (
			version int
			dirty   bool
		)
		if err := db.QueryRow(`
			SELECT version, dirty FROM chain.schema_migrations
		`).Scan(&version, &dirty); err != nil {
			t.Fatalf("read failed migration version: %v", err)
		}

		if version != 1 || !dirty {
			t.Fatalf("failed migration state: version=%d dirty=%t; want version=1 dirty=true",
				version, dirty)
		}
	})

	t.Run("pre-existing dirty version is fatal", func(t *testing.T) {
		db := newMigrationTestDatabase(t)
		initializeSnapshot(t, db)

		if _, err := db.Exec(`
			CREATE TABLE chain.schema_migrations (
				version BIGINT NOT NULL PRIMARY KEY,
				dirty BOOLEAN NOT NULL
			);
			INSERT INTO chain.schema_migrations (version, dirty) VALUES (1, TRUE)
		`); err != nil {
			t.Fatalf("seed dirty migration version: %v", err)
		}

		err := syncerdatabase.RunMigrations(db, nil)
		if err == nil || !strings.Contains(err.Error(), "Dirty database version 1") {
			t.Fatalf("expected dirty-version failure, got %v", err)
		}
	})
}

func newMigrationTestDatabase(t *testing.T) *sql.DB {
	t.Helper()

	baseDSN := framework.DefaultFrameworkConfig().DB.ConnString()

	admin, err := sql.Open("postgres", baseDSN)
	if err != nil {
		t.Fatalf("open migration admin database: %v", err)
	}

	name := fmt.Sprintf("syncer_migration_%d", time.Now().UTC().UnixNano())
	if _, err := admin.Exec(`CREATE DATABASE "` + name + `"`); err != nil {
		_ = admin.Close()

		t.Fatalf("create migration test database %s: %v", name, err)
	}

	parsed, err := url.Parse(baseDSN)
	if err != nil {
		_ = admin.Close()

		t.Fatalf("parse migration database DSN: %v", err)
	}

	parsed.Path = "/" + name
	parsed.RawPath = ""

	db, err := sql.Open("postgres", parsed.String())
	if err != nil {
		_ = admin.Close()

		t.Fatalf("open migration test database: %v", err)
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		_ = admin.Close()

		t.Fatalf("ping migration test database: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()

		_, _ = admin.Exec(`
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1 AND pid <> pg_backend_pid()
		`, name)
		if _, err := admin.Exec(`DROP DATABASE IF EXISTS "` + name + `"`); err != nil {
			t.Errorf("drop migration test database %s: %v", name, err)
		}

		_ = admin.Close()
	})

	return db
}

func initializeSnapshot(t *testing.T, db *sql.DB) {
	t.Helper()

	snapshot, err := os.ReadFile("../scripts/init.sql")
	if err != nil {
		t.Fatalf("read database snapshot: %v", err)
	}

	if _, err := db.Exec(string(snapshot)); err != nil {
		t.Fatalf("initialize database snapshot: %v", err)
	}
}

func removeDataAnchorSchema(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.Exec(`
		DROP TABLE IF EXISTS chain.daily_commitment_stats;
		DROP TABLE IF EXISTS chain.data_anchor_factory_watchlist;
		DROP TABLE IF EXISTS chain.schema_migrations
	`); err != nil {
		t.Fatalf("remove post-migration schema: %v", err)
	}
}

func insertLegacyMigrationFixtures(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.Exec(`
		INSERT INTO chain.blocks (
			hash, number, parent_hash, nonce, sha3_uncles, logs_bloom,
			transactions_root, state_root, receipts_root, miner, difficulty,
			total_difficulty, extra_data, size, gas_limit, gas_used, timestamp,
			mix_hash, base_fee, txn_count
		) VALUES (
			'0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
			7,
			'0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
			'0x0',
			'0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
			'\x00',
			'0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
			'0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
			'0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
			'0x1111111111111111111111111111111111111111',
			1, 7, 'legacy', 100, 30000000, 21000, 1728000000,
			'0x1212121212121212121212121212121212121212121212121212121212121212',
			1, 1
		);

		INSERT INTO chain.transactions (
			hash, block_hash, block_number, from_address, to_address, value,
			nonce, gas_limit, gas_price, data, type, chain_id, status, block_timestamp
		) VALUES (
			'0x1313131313131313131313131313131313131313131313131313131313131313',
			'0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
			7,
			'0x1414141414141414141414141414141414141414',
			'0x1515151515151515151515151515151515151515',
			42, 1, 21000, 1, '0x', 0, '1', 'success', 1728000000
		);

		INSERT INTO chain.transaction_logs (
			tx_hash, log_index, block_number, address, topics, data
		) VALUES (
			'0x1313131313131313131313131313131313131313131313131313131313131313',
			0, 7, '0x1616161616161616161616161616161616161616',
			ARRAY['0x1717171717171717171717171717171717171717171717171717171717171717'],
			'0x'
		);

		INSERT INTO chain.metadata (key, value)
		VALUES ('txworker_last_block_processed', '7');

		INSERT INTO chain.erc20_watchlist (
			address, symbol, decimals, enabled, is_private, next_block
		) VALUES (
			'0x1818181818181818181818181818181818181818',
			'LEGACY', 18, TRUE, FALSE, 8
		);

		INSERT INTO chain.erc20_hourly_stats (
			token_address, hour_utc, transfer_count, transfer_volume_raw,
			mint_count, mint_volume_raw, burn_count, burn_volume_raw,
			cumulative_circulation
		) VALUES (
			'0x1818181818181818181818181818181818181818',
			'2024-10-04 00:00:00+00', 1, 42, 1, 100, 0, 0, 100
		);

		INSERT INTO chain.entity_hour_participation (hour_utc, address)
		VALUES ('2024-10-04 00:00:00+00', '0x1919191919191919191919191919191919191919');

		INSERT INTO chain.validator_metadata (address, name, institution, region)
		VALUES (
			'0x2020202020202020202020202020202020202020',
			'Legacy Validator', 'Legacy Institution', 'EU'
		);

		INSERT INTO chain.asset_issuers (id, name, website, contact, region)
		VALUES (
			'11111111-1111-1111-1111-111111111111',
			'Legacy Issuer', 'https://legacy.example', 'legacy@example.com', 'EU'
		);

		INSERT INTO chain.asset_issuer_tokens (issuer_id, token_address)
		VALUES (
			'11111111-1111-1111-1111-111111111111',
			'0x1818181818181818181818181818181818181818'
		);

		INSERT INTO chain.esg_state (
			time_at, total_lbm_carbon_emissions, total_mbm_carbon_emissions
		) VALUES ('2024-10-04 00:00:00', 1.25, 2.50)
	`); err != nil {
		t.Fatalf("insert populated pre-migration fixtures: %v", err)
	}
}

func legacyMigrationSnapshot(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()

	tables := []string{
		"blocks",
		"transactions",
		"transaction_logs",
		"metadata",
		"erc20_watchlist",
		"erc20_hourly_stats",
		"entity_hour_participation",
		"validator_metadata",
		"asset_issuers",
		"asset_issuer_tokens",
		"esg_state",
	}
	snapshot := make(map[string]string, len(tables))

	for _, table := range tables {
		var rows string

		query := fmt.Sprintf(`
			SELECT COALESCE(
				jsonb_agg(to_jsonb(row_data) ORDER BY to_jsonb(row_data)::TEXT),
				'[]'::JSONB
			)::TEXT
			FROM chain.%s AS row_data
		`, table)
		if err := db.QueryRow(query).Scan(&rows); err != nil {
			t.Fatalf("snapshot legacy table %s: %v", table, err)
		}

		snapshot[table] = rows
	}

	return snapshot
}

func assertDataAnchorMigrationState(t *testing.T, db *sql.DB) {
	t.Helper()

	var (
		version int
		dirty   bool
	)
	if err := db.QueryRow(`
		SELECT version, dirty FROM chain.schema_migrations
	`).Scan(&version, &dirty); err != nil {
		t.Fatalf("read migration version: %v", err)
	}

	if version != 1 || dirty {
		t.Fatalf("migration state: version=%d dirty=%t; want version=1 dirty=false",
			version, dirty)
	}

	for _, table := range []string{
		"data_anchor_factory_watchlist",
		"daily_commitment_stats",
	} {
		var exists bool
		if err := db.QueryRow(`
			SELECT to_regclass($1) IS NOT NULL
		`, "chain."+table).Scan(&exists); err != nil {
			t.Fatalf("check migrated table %s: %v", table, err)
		}

		if !exists {
			t.Fatalf("migrated table %s does not exist", table)
		}
	}
}

func assertNoDataAnchorSchema(t *testing.T, db *sql.DB) {
	t.Helper()

	for _, table := range []string{
		"data_anchor_factory_watchlist",
		"daily_commitment_stats",
		"schema_migrations",
	} {
		var exists bool
		if err := db.QueryRow(`
			SELECT to_regclass($1) IS NOT NULL
		`, "chain."+table).Scan(&exists); err != nil {
			t.Fatalf("check pre-migration table %s: %v", table, err)
		}

		if exists {
			t.Fatalf("legacy snapshot unexpectedly contains chain.%s", table)
		}
	}
}

func assertNoTransactionLogsLookupIndexes(t *testing.T, db *sql.DB) {
	t.Helper()

	for _, index := range []string{
		"idx_transaction_logs_block_address",
		"idx_transaction_logs_topics_gin",
	} {
		var exists bool
		if err := db.QueryRow(`
			SELECT EXISTS (
				SELECT 1
				FROM pg_indexes
				WHERE schemaname = 'chain' AND indexname = $1
			)
		`, index).Scan(&exists); err != nil {
			t.Fatalf("check index %s: %v", index, err)
		}

		if exists {
			t.Fatalf("lookup index %s must remain a manual deployment step", index)
		}
	}
}
