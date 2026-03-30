package api_storage

import (
	"database/sql"
	"sync"
)

var (
	dbMu sync.RWMutex
	db   *sql.DB
)

// SetDB sets the PostgreSQL connection used by all api_storage queries. Call once at API startup.
func SetDB(conn *sql.DB) {
	dbMu.Lock()
	defer dbMu.Unlock()
	db = conn
}

func getDB() *sql.DB {
	dbMu.RLock()
	defer dbMu.RUnlock()
	return db
}
