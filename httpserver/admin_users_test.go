package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alexedwards/scs/v2"
)

func TestHandleAdminUsers_InvalidMethod(t *testing.T) {
	t.Parallel()
	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret"},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminUsers))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PATCH", "/admin/v1/users", nil)
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestHandleAdminUsers_PutNoID(t *testing.T) {
	t.Parallel()
	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret"},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminUsers))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/admin/v1/users", strings.NewReader(`{"username":"test"}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleAdminUsers_DeleteNoID(t *testing.T) {
	t.Parallel()
	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret"},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(s.handleAdminUsers))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("DELETE", "/admin/v1/users", nil)
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleCreateAdminUser_EmptyUsername(t *testing.T) {
	t.Parallel()
	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret"},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleCreateAdminUser(w, r)
	})))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/v1/users",
		strings.NewReader(`{"username":"","password":"pass"}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleCreateAdminUser_EmptyPassword(t *testing.T) {
	t.Parallel()
	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret"},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleCreateAdminUser(w, r)
	})))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/v1/users",
		strings.NewReader(`{"username":"admin","password":""}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleCreateAdminUser_InvalidJSON(t *testing.T) {
	t.Parallel()
	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret"},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleCreateAdminUser(w, r)
	})))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/v1/users",
		strings.NewReader(`not json`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleUpdateAdminUser_EmptyUsername(t *testing.T) {
	t.Parallel()
	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret"},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleUpdateAdminUser(w, r, "some-id")
	})))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/admin/v1/users/some-id",
		strings.NewReader(`{"username":"","currentPassword":"old"}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleUpdateAdminUser_NoCurrentPassword(t *testing.T) {
	t.Parallel()
	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret"},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleUpdateAdminUser(w, r, "some-id")
	})))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/admin/v1/users/some-id",
		strings.NewReader(`{"username":"admin","currentPassword":""}`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleUpdateAdminUser_InvalidJSON(t *testing.T) {
	t.Parallel()
	sm := scs.New()
	s := &Server{
		cfg:            Config{AdminAPISecret: "secret"},
		sessionManager: sm,
	}

	handler := sm.LoadAndSave(s.requireAdmin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleUpdateAdminUser(w, r, "some-id")
	})))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/admin/v1/users/some-id",
		strings.NewReader(`not json`))
	req.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
