package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/alexedwards/scs/v2"
)

func TestHandleAdminErc20Watchlist_InvalidMethod(t *testing.T) {
	t.Parallel()
	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret"},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminErc20Watchlist))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/admin/v1/erc20/watchlist", nil)
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestHandleAdminErc20Watchlist_NoDB(t *testing.T) {
	t.Parallel()
	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret", DB: nil},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminErc20Watchlist))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/v1/erc20/watchlist",
		strings.NewReader(`{"address":"0x0000000000000000000000000000000000000001"}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestHandleAdminErc20Watchlist_InvalidJSON(t *testing.T) {
	t.Parallel()
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret", DB: db},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminErc20Watchlist))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/v1/erc20/watchlist",
		strings.NewReader(`not json`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleAdminErc20Watchlist_InvalidAddress(t *testing.T) {
	t.Parallel()
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret", DB: db},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminErc20Watchlist))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/v1/erc20/watchlist",
		strings.NewReader(`{"address":"not-an-address"}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleAdminErc20Watchlist_SymbolTooLong(t *testing.T) {
	t.Parallel()
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret", DB: db},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminErc20Watchlist))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/v1/erc20/watchlist",
		strings.NewReader(`{"address":"0x0000000000000000000000000000000000000001","symbol":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
