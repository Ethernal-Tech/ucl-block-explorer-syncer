package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alexedwards/scs/v2"
)

func TestHandleAdminValidators_NoDB(t *testing.T) {
	t.Parallel()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: secretStr, DB: nil},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminValidators))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/admin/v1/validators/0x0000000000000000000000000000000000000001",
		strings.NewReader(`{"name":"Test"}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestHandleAdminValidators_NoAddress(t *testing.T) {
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

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminValidators))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/admin/v1/validators/", strings.NewReader(`{"name":"Test"}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleAdminValidators_InvalidAddress(t *testing.T) {
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

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminValidators))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/admin/v1/validators/not-an-address",
		strings.NewReader(`{"name":"Test"}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleAdminValidators_InvalidMethod(t *testing.T) {
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

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminValidators))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PATCH", "/admin/v1/validators/0x0000000000000000000000000000000000000001", nil)
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestHandleUpsertValidator_InvalidJSON(t *testing.T) {
	t.Parallel()

	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: secretStr},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleUpsertValidator(w, r, "0x0000000000000000000000000000000000000001")
	})))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/admin/v1/validators/0x0000000000000000000000000000000000000001",
		strings.NewReader(`not json`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
