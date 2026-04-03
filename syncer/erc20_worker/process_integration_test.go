package erc20_worker

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/common"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	_ "github.com/lib/pq"
)

// openIntegrationPostgreSQL starts embedded-postgres (real server, ephemeral data under t.TempDir()).
// There is no SQLite-style in-memory Postgres; full PG syntax compatibility requires a real server.
func openIntegrationPostgreSQL(t *testing.T) *sql.DB {
	t.Helper()

	tmp := t.TempDir()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := uint32(ln.Addr().(*net.TCPAddr).Port)
	_ = ln.Close()

	cfg := embeddedpostgres.DefaultConfig().
		Port(port).
		RuntimePath(filepath.Join(tmp, "runtime")).
		DataPath(filepath.Join(tmp, "data")).
		Logger(io.Discard).
		StartTimeout(2 * time.Minute)

	pg := embeddedpostgres.NewDatabase(cfg)
	if err := pg.Start(); err != nil {
		t.Fatalf("embedded postgres: %v (first run may download Postgres binaries to the module cache)", err)
	}
	t.Cleanup(func() { _ = pg.Stop() })

	dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		_ = pg.Stop()
		t.Fatalf("sql open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return db
}

// repoRoot is the module root (directory containing go.mod), from this test file location.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}
	// syncer/erc20_worker/*.go -> ../.. -> module root
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

// stripSQLLineComments removes full-line `--` comments (scripts/init.sql style). Safe for this repo’s DDL.
func stripSQLLineComments(sql string) string {
	var b strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "--") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

func splitSQLStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// applyInitSQL runs scripts/init.sql from the repo: one Exec per statement (lib/pq does not accept
// multiple statements in a single Exec call).
func applyInitSQL(t *testing.T, db *sql.DB) {
	t.Helper()
	path := filepath.Join(repoRoot(t), "scripts", "init.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	body := stripSQLLineComments(string(raw))
	for i, stmt := range splitSQLStatements(body) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("init.sql statement %d (%s): %v\n%s", i+1, path, err, stmtPreview(stmt))
		}
	}
}

func stmtPreview(s string) string {
	const max = 400
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// TestInitSQLFileIsReadable ensures scripts/init.sql is found from this package and splits into
// statements (no Postgres required). Integration tests apply the same file via applyInitSQL.
func TestInitSQLFileIsReadable(t *testing.T) {
	path := filepath.Join(repoRoot(t), "scripts", "init.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	stmts := splitSQLStatements(stripSQLLineComments(string(raw)))
	if len(stmts) < 10 {
		t.Fatalf("expected init.sql to yield multiple statements, got %d", len(stmts))
	}
}

func TestIntegration_ProcessBlock_WatchlistAndUpsert(t *testing.T) {
	resetWatchlistCache()

	db := openIntegrationPostgreSQL(t)

	applyInitSQL(t, db)

	const token = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	_, err := db.Exec(`TRUNCATE chain.erc20_hourly_stats`)
	if err != nil {
		t.Fatalf("truncate stats: %v", err)
	}
	_, err = db.Exec(`DELETE FROM chain.erc20_watchlist`)
	if err != nil {
		t.Fatalf("delete watchlist: %v", err)
	}
	_, err = db.Exec(`INSERT INTO chain.erc20_watchlist (address, symbol, enabled) VALUES ($1, 'TT', true)`, token)
	if err != nil {
		t.Fatalf("insert watchlist: %v", err)
	}

	zero := common.Address{}
	peer := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	topicsMint := []string{TransferTopic.Hex(), addrTopicHex(zero), addrTopicHex(peer)}
	data := uint256DataHex(big.NewInt(42))

	job := BlockJob{
		BlockNumber: 50,
		BlockTS:     1704067200, // 2024-01-01 00:00:00 UTC
		Txs: []*types.Transaction{
			{
				Hash: "0x01",
				Logs: []types.ReceiptLog{
					{Address: token, Topics: topicsMint, Data: data},
				},
			},
		},
	}

	ctx := context.Background()
	if err := ProcessBlock(ctx, db, job); err != nil {
		t.Fatalf("ProcessBlock: %v", err)
	}

	var mintCount int64
	var mintVol string
	err = db.QueryRow(`
		SELECT mint_count, mint_volume_raw::text
		FROM chain.erc20_hourly_stats
		WHERE lower(token_address) = lower($1) AND hour_utc = '2024-01-01T00:00:00Z'::timestamptz
	`, token).Scan(&mintCount, &mintVol)
	if err != nil {
		t.Fatalf("select stats: %v", err)
	}
	if mintCount != 1 || mintVol != "42" {
		t.Fatalf("unexpected row: mint_count=%d mint_volume_raw=%s", mintCount, mintVol)
	}

	// Second identical job: ON CONFLICT should add counts/volumes.
	if err := ProcessBlock(ctx, db, job); err != nil {
		t.Fatalf("ProcessBlock 2: %v", err)
	}
	err = db.QueryRow(`
		SELECT mint_count, mint_volume_raw::text
		FROM chain.erc20_hourly_stats
		WHERE lower(token_address) = lower($1) AND hour_utc = '2024-01-01T00:00:00Z'::timestamptz
	`, token).Scan(&mintCount, &mintVol)
	if err != nil {
		t.Fatalf("select stats 2: %v", err)
	}
	if mintCount != 2 || mintVol != "84" {
		t.Fatalf("after upsert: mint_count=%d mint_volume_raw=%s", mintCount, mintVol)
	}
}

func TestIntegration_ReloadWatchlistAcrossEpoch(t *testing.T) {
	resetWatchlistCache()

	db := openIntegrationPostgreSQL(t)

	applyInitSQL(t, db)

	const token = "0xdddddddddddddddddddddddddddddddddddddddd"
	_, err := db.Exec(`TRUNCATE chain.erc20_hourly_stats`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
	_, err = db.Exec(`DELETE FROM chain.erc20_watchlist`)
	if err != nil {
		t.Fatalf("delete watchlist: %v", err)
	}

	// Block 0: empty watchlist → ProcessBlock no-ops without error.
	job0 := BlockJob{BlockNumber: 0, BlockTS: 1, Txs: []*types.Transaction{{Hash: "0x1", Logs: []types.ReceiptLog{}}}}
	if err := ProcessBlock(context.Background(), db, job0); err != nil {
		t.Fatalf("ProcessBlock empty watchlist: %v", err)
	}

	// Insert token and move to block 100 (new epoch for watchlist cache) so reload sees it.
	_, err = db.Exec(`INSERT INTO chain.erc20_watchlist (address, enabled) VALUES ($1, true)`, token)
	if err != nil {
		t.Fatalf("insert watchlist: %v", err)
	}

	peer := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	topics := []string{TransferTopic.Hex(), addrTopicHex(peer), addrTopicHex(peer)}
	job100 := BlockJob{
		BlockNumber: 100,
		BlockTS:     1704067200,
		Txs: []*types.Transaction{
			{
				Hash: "0x02",
				Logs: []types.ReceiptLog{
					{Address: token, Topics: topics, Data: uint256DataHex(big.NewInt(3))},
				},
			},
		},
	}
	if err := ProcessBlock(context.Background(), db, job100); err != nil {
		t.Fatalf("ProcessBlock: %v", err)
	}

	var tc int64
	var tv string
	err = db.QueryRow(`
		SELECT transfer_count, transfer_volume_raw::text
		FROM chain.erc20_hourly_stats
		WHERE lower(token_address) = lower($1) AND hour_utc = '2024-01-01T00:00:00Z'::timestamptz
	`, token).Scan(&tc, &tv)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if tc != 1 || tv != "3" {
		t.Fatalf("expected transfer_count=1 vol=3, got %d %s", tc, tv)
	}
}
