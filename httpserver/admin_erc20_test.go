package httpserver

import (
	"net/http/httptest"
	"strings"
	"testing"
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
	s := &Server{cfg: Config{AdminAPISecret: ""}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/v1/erc20/watchlist", strings.NewReader(`{}`))
	s.handleAdminErc20Watchlist(rec, req)
	if rec.Code != 404 {
		t.Fatalf("status %d", rec.Code)
	}
}
