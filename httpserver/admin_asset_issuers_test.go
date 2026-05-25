package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/alexedwards/scs/v2"
)

func TestHandleCreateAssetIssuer_EmptyName(t *testing.T) {
	t.Parallel()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: secretStr},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleCreateAssetIssuer(w, r)
	})))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/v1/asset-issuers",
		strings.NewReader(`{"name":"","region":"Belgrade"}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleCreateAssetIssuer_InvalidJSON(t *testing.T) {
	t.Parallel()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: secretStr},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleCreateAssetIssuer(w, r)
	})))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/v1/asset-issuers",
		strings.NewReader(`not json`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleAdminAssetIssuers_NoDB(t *testing.T) {
	t.Parallel()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: secretStr, DB: nil},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminAssetIssuers))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/v1/asset-issuers",
		strings.NewReader(`{"name":"Test"}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestHandleAdminAssetIssuers_DeleteNoID(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: secretStr, DB: db},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminAssetIssuers))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("DELETE", "/admin/v1/asset-issuers", nil)
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleAdminAssetIssuers_PutNoID(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: secretStr, DB: db},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminAssetIssuers))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/admin/v1/asset-issuers", strings.NewReader(`{"name":"Test"}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
