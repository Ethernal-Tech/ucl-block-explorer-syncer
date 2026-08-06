package database

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestDataAnchorMigrationIsEmbeddedAndAdditive(t *testing.T) {
	t.Parallel()

	up, err := migrationFiles.ReadFile("migrations/000001_data_anchor_stats.up.sql")
	if err != nil {
		t.Fatalf("read embedded up migration: %v", err)
	}

	if _, err := migrationFiles.ReadFile("migrations/000001_data_anchor_stats.down.sql"); err != nil {
		t.Fatalf("read embedded down migration: %v", err)
	}

	sql := string(up)

	required := []string{
		"CREATE SCHEMA IF NOT EXISTS chain",
		"CREATE TABLE IF NOT EXISTS chain.data_anchor_factory_watchlist",
		"CREATE TABLE IF NOT EXISTS chain.daily_commitment_stats",
		"CREATE INDEX IF NOT EXISTS idx_daily_commitment_stats_factory_day",
	}
	for _, statement := range required {
		if !strings.Contains(sql, statement) {
			t.Fatalf("migration is missing %q", statement)
		}
	}

	upper := strings.ToUpper(sql)
	for _, forbidden := range []string{
		"DROP TABLE",
		"TRUNCATE ",
		"DELETE FROM",
		"ALTER TABLE",
		"IDX_TRANSACTION_LOGS_BLOCK_ADDRESS",
		"IDX_TRANSACTION_LOGS_TOPICS_GIN",
		"CREATE INDEX CONCURRENTLY",
	} {
		if strings.Contains(upper, forbidden) {
			t.Fatalf("migration contains forbidden statement %q", forbidden)
		}
	}
}

func TestInitSQLDoesNotOwnDataAnchorSchema(t *testing.T) {
	t.Parallel()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}

	snapshotPath := filepath.Join(filepath.Dir(file), "..", "scripts", "init.sql")

	snapshot, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read init.sql snapshot: %v", err)
	}

	sql := string(snapshot)
	for _, featureOwned := range []string{
		"data_anchor_factory_watchlist",
		"daily_commitment_stats",
		"idx_daily_commitment_stats_",
		"idx_transaction_logs_block_address",
		"idx_transaction_logs_topics_gin",
	} {
		if strings.Contains(sql, featureOwned) {
			t.Fatalf("scripts/init.sql must remain legacy-only; found %q", featureOwned)
		}
	}
}
