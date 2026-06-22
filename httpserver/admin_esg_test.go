package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleAdminEsg_NoSecret(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: ""}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/esg", nil)

	s.handleAdminEsg(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestHandleAdminEsg_WrongToken(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: "secret"}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/esg", nil)
	req.Header.Set("Authorization", "Bearer wrong")

	s.handleAdminEsg(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandleAdminEsg_MissingToken(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: "secret"}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/esg", nil)

	s.handleAdminEsg(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandleAdminEsg_NoDB(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: "secret", DB: nil}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/esg", nil)
	req.Header.Set("Authorization", "Bearer secret")

	s.handleAdminEsg(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}
