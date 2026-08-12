package api_storage

import (
	"database/sql"
	"sync"
	"time"
)

const (
	dataAnchorSchemaProbeInterval = 5 * time.Second
	isDataAnchorSQLExprDisabled   = "FALSE"
)

var (
	db *sql.DB

	dataAnchorMu        sync.Mutex
	dataAnchorReady     = true // assume ready until SetDB probes (keeps unit tests simple)
	dataAnchorLastProbe time.Time
)

// SetDB sets the PostgreSQL connection used by all api_storage queries. Call once at API startup.
// It also probes whether data-anchor migration tables exist so transaction list/by-hash queries
// can omit those joins until the syncer has applied migrations.
func SetDB(conn *sql.DB) {
	db = conn

	dataAnchorMu.Lock()
	defer dataAnchorMu.Unlock()

	refreshDataAnchorSchemaLocked(conn)
}

func getDB() *sql.DB {
	return db
}

// dataAnchorSQLExpr returns the SQL boolean expression used for is_data_anchor.
// When migration tables are missing it returns FALSE so core transaction APIs
// do not fail; while disabled it re-probes periodically so a later syncer
// migration is picked up without an API restart.
func dataAnchorSQLExpr() string {
	dataAnchorMu.Lock()
	defer dataAnchorMu.Unlock()

	if dataAnchorReady {
		return isDataAnchorSQLExprEnabled
	}

	if time.Since(dataAnchorLastProbe) >= dataAnchorSchemaProbeInterval {
		refreshDataAnchorSchemaLocked(getDB())
	}

	if dataAnchorReady {
		return isDataAnchorSQLExprEnabled
	}

	return isDataAnchorSQLExprDisabled
}

func refreshDataAnchorSchemaLocked(conn *sql.DB) {
	dataAnchorLastProbe = time.Now().UTC()
	dataAnchorReady = dataAnchorTablesReady(conn)
}

func dataAnchorTablesReady(conn *sql.DB) bool {
	if conn == nil {
		return false
	}

	var ready bool

	err := conn.QueryRow(`
		SELECT to_regclass('chain.data_anchor_factory_watchlist') IS NOT NULL
			AND to_regclass('chain.daily_commitment_stats') IS NOT NULL
	`).Scan(&ready)

	return err == nil && ready
}

// resetDataAnchorSchemaStateForTest restores the package default used by unit tests
// that assign db directly without calling SetDB.
func resetDataAnchorSchemaStateForTest() {
	dataAnchorMu.Lock()
	defer dataAnchorMu.Unlock()

	dataAnchorReady = true
	dataAnchorLastProbe = time.Time{}
}
