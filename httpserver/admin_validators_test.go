package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleAdminValidators_NoSecret(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: ""}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut,
		"/admin/v1/validators/0xAbC1234567890000000000000000000000000001",
		strings.NewReader(`{}`))

	s.handleAdminValidators(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when no secret, got %d", rec.Code)
	}
}

func TestHandleAdminValidators_WrongToken(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: "secret"}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut,
		"/admin/v1/validators/0xAbC1234567890000000000000000000000000001",
		strings.NewReader(`{"name":"test"}`))
	req.Header.Set("Authorization", "Bearer wrong-token")

	s.handleAdminValidators(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandleAdminValidators_MissingToken(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: "secret"}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut,
		"/admin/v1/validators/0xAbC1234567890000000000000000000000000001",
		strings.NewReader(`{"name":"test"}`))
	// no Authorization header

	s.handleAdminValidators(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandleAdminValidators_NoDB(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: "secret", DB: nil}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut,
		"/admin/v1/validators/0xAbC1234567890000000000000000000000000001",
		strings.NewReader(`{"name":"test"}`))
	req.Header.Set("Authorization", "Bearer secret")

	s.handleAdminValidators(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestHandleUpsertValidator_InvalidJSON(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(`{bad json`))

	s.handleUpsertValidator(rec, req, "0xAbC1234567890000000000000000000000000001")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad JSON, got %d", rec.Code)
	}
}

func TestHandleUpsertValidator_EmptyBody(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(``))

	s.handleUpsertValidator(rec, req, "0xAbC1234567890000000000000000000000000001")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty body, got %d", rec.Code)
	}
}

func TestHandleUpsertValidator_InvalidAddress(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(`{"name":"test"}`))

	s.handleUpsertValidator(rec, req, "not-an-address")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid address, got %d", rec.Code)
	}
}
