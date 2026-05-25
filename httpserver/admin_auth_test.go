package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alexedwards/scs/v2"
)

func TestParseBearerToken(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest("POST", "/", nil)
	req.Header.Set("Authorization", "Bearer secret-token")

	if got := parseBearerToken(req); got != "secret-token" {
		t.Fatalf("got %q", got)
	}

	req.Header.Set("Authorization", "bearer lower")

	if got := parseBearerToken(req); got != "lower" {
		t.Fatalf("case: got %q", got)
	}

	req.Header.Set("Authorization", "Basic x")

	if got := parseBearerToken(req); got != "" {
		t.Fatalf("wrong scheme: got %q", got)
	}
}

func TestConstantTimeEqualString(t *testing.T) {
	t.Parallel()

	if !constantTimeEqualString("a", "a") {
		t.Fatal("equal strings")
	}

	if constantTimeEqualString("a", "b") {
		t.Fatal("different strings")
	}

	if constantTimeEqualString("a", "aa") {
		t.Fatal("different lengths")
	}
}

func TestHandleAdminErc20Watchlist_NoSecret(t *testing.T) {
	t.Parallel()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: ""},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminErc20Watchlist))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/v1/erc20/watchlist", strings.NewReader(`{}`))
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestRequireAdmin_NoSecret(t *testing.T) {
	t.Parallel()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: ""},
		sessionManager: sm,
	}

	dummy := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}

	handler := sm.LoadAndSave(s.requireAdmin(dummy))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/admin/test", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestRequireAdmin_WrongBearer(t *testing.T) {
	t.Parallel()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "correct-secret"},
		sessionManager: sm,
	}

	dummy := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}

	handler := sm.LoadAndSave(s.requireAdmin(dummy))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/admin/test", nil)
	req.Header.Set("Authorization", "Bearer wrong-secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestRequireAdmin_ValidBearer(t *testing.T) {
	t.Parallel()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "my-secret"},
		sessionManager: sm,
	}

	dummy := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}

	handler := sm.LoadAndSave(s.requireAdmin(dummy))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/admin/test", nil)
	req.Header.Set("Authorization", "Bearer my-secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestRequireAdmin_NoBearer_NoSession(t *testing.T) {
	t.Parallel()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "my-secret"},
		sessionManager: sm,
	}

	dummy := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}

	handler := sm.LoadAndSave(s.requireAdmin(dummy))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/admin/test", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}
