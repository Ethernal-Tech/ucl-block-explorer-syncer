package api_storage

import (
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func integrationCirculationDSN() string {
	if dsn := os.Getenv("CIRCULATION_TEST_DATABASE_URL"); dsn != "" {
		return dsn
	}
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		return dsn
	}
	return "postgres://postgres:postgres@127.0.0.1:5432/explorer?sslmode=disable"
}

func TestGetErc20CirculationCumulative_Integration(t *testing.T) {
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
		t.Skipf("postgres not reachable: %v", err)
	}

	SetDB(db)

	logHourlyStatsContext(t, db)

	for _, g := range []string{"hour", "day", "month"} {
		t.Run("granularity_"+g, func(t *testing.T) {
			resp, err := GetErc20CirculationCumulativeStats(Erc20CirculationCumulativeRequest{
				Granularity: g,
				Page:        1,
				PageSize:    10,
			})
			if err != nil {
				t.Fatalf("GetErc20CirculationCumulativeStats(%s): %v", g, err)
			}
			t.Logf("granularity=%s code=%s total=%d rows=%d",
				g, resp.Code, resp.Data.Total, len(resp.Data.List))
			for i, row := range resp.Data.List {
				t.Logf("  [%d] bucket=%s total=%s", i, row.BucketUtc, trimForLog(row.Total))
			}
		})
	}
}

func logHourlyStatsContext(t *testing.T, db *sql.DB) {
	t.Helper()

	var rawMin, rawMax sql.NullTime
	err := db.QueryRow(`SELECT MIN(hour_utc), MAX(hour_utc) FROM chain.erc20_hourly_stats`).Scan(&rawMin, &rawMax)
	if err != nil {
		t.Logf("erc20_hourly_stats: scan error: %v", err)
	} else {
		t.Logf("erc20_hourly_stats: min=%v max=%v", nullTimeStr(rawMin), nullTimeStr(rawMax))
	}

	var watchRows int
	_ = db.QueryRow(`SELECT COUNT(*) FROM chain.erc20_watchlist WHERE enabled = true AND decimals IS NOT NULL`).Scan(&watchRows)
	t.Logf("erc20_watchlist (enabled + decimals): %d", watchRows)

	rows, err := db.Query(`
		SELECT s.hour_utc, s.token_address,
		       s.mint_volume_raw::text, s.burn_volume_raw::text,
		       s.cumulative_circulation::text
		FROM chain.erc20_hourly_stats s
		INNER JOIN chain.erc20_watchlist w ON lower(w.address) = lower(s.token_address)
		WHERE w.enabled = true AND w.decimals IS NOT NULL
		ORDER BY s.hour_utc ASC
		LIMIT 5
	`)
	if err != nil {
		t.Logf("sample query: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var hourUtc time.Time
		var addr, mint, burn, circ string
		if err := rows.Scan(&hourUtc, &addr, &mint, &burn, &circ); err != nil {
			t.Logf("scan: %v", err)
			return
		}
		t.Logf("  hour=%s token=%s mint_raw=%s burn_raw=%s circulation=%s",
			hourUtc.UTC().Format(time.RFC3339), addr,
			trimForLog(mint), trimForLog(burn), trimForLog(circ))
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
