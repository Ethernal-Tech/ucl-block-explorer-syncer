package api_storage

import (
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// integrationCirculationDSN returns the Postgres URL for local docker-compose (see repo docker-compose.yml).
// Override with CIRCULATION_TEST_DATABASE_URL or DATABASE_URL if needed.
func integrationCirculationDSN() string {
	if dsn := os.Getenv("CIRCULATION_TEST_DATABASE_URL"); dsn != "" {
		return dsn
	}
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		return dsn
	}
	// docker-compose: postgres:5432 -> host 127.0.0.1:5432, db explorer, user postgres/postgres
	return "postgres://postgres:postgres@127.0.0.1:5432/explorer?sslmode=disable"
}

// TestEnsureCirculationCacheThroughLastCompleteHour_Integration opens a real DB and runs the cache
// extension once. Disabled unless CIRCULATION_INTEGRATION_DEBUG=1 (manual / launch.json only).
//
//	CIRCULATION_INTEGRATION_DEBUG=1 go test ./api_storage -run TestEnsureCirculationCacheThroughLastCompleteHour_Integration -v
//
// Skips if Postgres is unreachable. Start DB: docker compose up -d postgres
func TestEnsureCirculationCacheThroughLastCompleteHour_Integration(t *testing.T) {
	if os.Getenv("CIRCULATION_INTEGRATION_DEBUG") != "1" {
		t.Skip("set CIRCULATION_INTEGRATION_DEBUG=1 to run this debug-only test")
	}

	dsn := integrationCirculationDSN()
	t.Logf("dsn: %s", dsn)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql open: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(2)
	db.SetConnMaxLifetime(time.Minute)

	if err := db.Ping(); err != nil {
		t.Skipf("postgres not reachable (docker compose up -d postgres?): %v", err)
	}

	logCirculationIntegrationContext(t, db)

	if err := ensureCirculationCacheThroughLastCompleteHour(db); err != nil {
		t.Fatalf("ensureCirculationCacheThroughLastCompleteHour: %v", err)
	}

	t.Log("ensureCirculationCacheThroughLastCompleteHour: ok")
	logCirculationCumulativeSample(t, db)
}

func logCirculationIntegrationContext(t *testing.T, db *sql.DB) {
	t.Helper()

	var rawMin, rawMax sql.NullTime
	err := db.QueryRow(`SELECT MIN(hour_utc), MAX(hour_utc) FROM chain.erc20_hourly_stats`).Scan(&rawMin, &rawMax)
	if err != nil {
		t.Logf("erc20_hourly_stats (all): scan error: %v", err)
	} else {
		t.Logf("erc20_hourly_stats (all): min=%v max=%v", nullTimeStr(rawMin), nullTimeStr(rawMax))
	}

	var wlMin, wlMax sql.NullTime
	err = db.QueryRow(`
		SELECT MIN(s.hour_utc), MAX(s.hour_utc)
		FROM chain.erc20_hourly_stats s
		INNER JOIN chain.erc20_watchlist w ON lower(w.address) = lower(s.token_address)
		WHERE w.enabled = true AND w.decimals IS NOT NULL
	`).Scan(&wlMin, &wlMax)
	if err != nil {
		t.Logf("erc20_hourly_stats (watchlist+decimals): scan error: %v", err)
	} else {
		t.Logf("erc20_hourly_stats (watchlist+decimals): min=%v max=%v", nullTimeStr(wlMin), nullTimeStr(wlMax))
	}

	var watchRows int
	_ = db.QueryRow(`SELECT COUNT(*) FROM chain.erc20_watchlist WHERE enabled = true AND decimals IS NOT NULL`).Scan(&watchRows)
	t.Logf("erc20_watchlist rows with enabled=true AND decimals IS NOT NULL: %d", watchRows)

	if wlMin.Valid {
		h := wlMin.Time.UTC()
		var netStr sql.NullString
		q := `
			SELECT COALESCE(SUM(
				(s.mint_volume_raw - s.burn_volume_raw) / power(10::numeric, w.decimals)
			), 0)::text
			FROM chain.erc20_hourly_stats s
			INNER JOIN chain.erc20_watchlist w ON lower(w.address) = lower(s.token_address)
			WHERE w.enabled = true AND w.decimals IS NOT NULL AND s.hour_utc = $1::timestamptz
		`
		if err := db.QueryRow(q, h.Format(time.RFC3339)).Scan(&netStr); err != nil {
			t.Logf("net at first watchlist hour %v: %v", h, err)
		} else {
			t.Logf("mint−burn net (human) at first watchlist hour %v: %s", h, netStr.String)
		}
	}

	var cMin, cMax sql.NullTime
	err = db.QueryRow(`SELECT MIN(hour_utc), MAX(hour_utc) FROM chain.erc20_circulation_cumulative`).Scan(&cMin, &cMax)
	if err != nil {
		t.Logf("erc20_circulation_cumulative: scan error: %v", err)
	} else {
		t.Logf("erc20_circulation_cumulative (before): min=%v max=%v", nullTimeStr(cMin), nullTimeStr(cMax))
	}
}

func logCirculationCumulativeSample(t *testing.T, db *sql.DB) {
	t.Helper()

	var cMin, cMax sql.NullTime
	if err := db.QueryRow(`SELECT MIN(hour_utc), MAX(hour_utc) FROM chain.erc20_circulation_cumulative`).Scan(&cMin, &cMax); err != nil {
		t.Logf("erc20_circulation_cumulative (after): scan error: %v", err)
		return
	}
	t.Logf("erc20_circulation_cumulative (after): min=%v max=%v", nullTimeStr(cMin), nullTimeStr(cMax))

	rows, err := db.Query(`
		SELECT hour_utc, cumulative_total::text
		FROM chain.erc20_circulation_cumulative
		ORDER BY hour_utc ASC
		LIMIT 5
	`)
	if err != nil {
		t.Logf("sample first rows: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var ts time.Time
		var total string
		if err := rows.Scan(&ts, &total); err != nil {
			t.Logf("scan: %v", err)
			return
		}
		t.Logf("  first: hour_utc=%s cumulative_total=%s", ts.UTC().Format(time.RFC3339), trimForLog(total))
	}

	rows2, err := db.Query(`
		SELECT hour_utc, cumulative_total::text
		FROM chain.erc20_circulation_cumulative
		ORDER BY hour_utc DESC
		LIMIT 3
	`)
	if err != nil {
		return
	}
	defer rows2.Close()
	for rows2.Next() {
		var ts time.Time
		var total string
		if err := rows2.Scan(&ts, &total); err != nil {
			return
		}
		t.Logf("  last: hour_utc=%s cumulative_total=%s", ts.UTC().Format(time.RFC3339), trimForLog(total))
	}
}

func nullTimeStr(nt sql.NullTime) string {
	if !nt.Valid {
		return "<null>"
	}
	return nt.Time.UTC().Format(time.RFC3339)
}

func trimForLog(s string) string {
	if len(s) > 80 {
		return s[:77] + "..."
	}
	return s
}
