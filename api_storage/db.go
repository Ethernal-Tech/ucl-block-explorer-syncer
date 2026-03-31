package api_storage

import (
	"database/sql"
)

var (
	db *sql.DB
)

// SetDB sets the PostgreSQL connection used by all api_storage queries. Call once at API startup.
func SetDB(conn *sql.DB) {
	db = conn
}

func getDB() *sql.DB {
	return db
}
