package httpserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleAdminAssetIssuers_NoSecret(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: ""}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/asset-issuers",
		strings.NewReader(`{"name":"test"}`))

	s.handleAdminAssetIssuers(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestHandleAdminAssetIssuers_WrongToken(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: "secret"}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/asset-issuers",
		strings.NewReader(`{"name":"test"}`))
	req.Header.Set("Authorization", "Bearer wrong")

	s.handleAdminAssetIssuers(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandleAdminAssetIssuers_MissingToken(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: "secret"}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/asset-issuers",
		strings.NewReader(`{"name":"test"}`))

	s.handleAdminAssetIssuers(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandleAdminAssetIssuers_NoDB(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{AdminAPISecret: "secret", DB: nil}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/asset-issuers",
		strings.NewReader(`{"name":"test"}`))
	req.Header.Set("Authorization", "Bearer secret")

	s.handleAdminAssetIssuers(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestHandleCreateAssetIssuer_InvalidJSON(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{bad`))

	s.handleCreateAssetIssuer(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleCreateAssetIssuer_EmptyName(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":""}`))

	s.handleCreateAssetIssuer(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty name, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("response not valid JSON: %v", err)
	}

	if !strings.Contains(body["error"], "name") {
		t.Fatalf("expected 'name' in error response, got: %s", body["error"])
	}
}

func TestHandleCreateAssetIssuer_WhitespaceName(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"   "}`))

	s.handleCreateAssetIssuer(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for whitespace-only name, got %d", rec.Code)
	}
}

func TestHandleCreateAssetIssuer_InvalidAssetAddress(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(
		`{"name":"Issuer","assets":["not-an-address"]}`,
	))

	s.handleCreateAssetIssuer(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid asset address, got %d", rec.Code)
	}
}

func TestHandleCreateAssetIssuer_InvalidAssetAddressInList(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(
		`{"name":"Issuer","assets":["0xAbC1234567890000000000000000000000000001","bad"]}`,
	))

	s.handleCreateAssetIssuer(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad address in list, got %d", rec.Code)
	}
}

func TestHandleUpdateAssetIssuer_InvalidJSON(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(`{bad`))

	s.handleUpdateAssetIssuer(rec, req, "some-id")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleUpdateAssetIssuer_EmptyName(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(`{"name":""}`))

	s.handleUpdateAssetIssuer(rec, req, "some-id")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty name, got %d", rec.Code)
	}
}

func TestHandleUpdateAssetIssuer_InvalidAssetAddress(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(
		`{"name":"Issuer","assets":["0xinvalid"]}`,
	))

	s.handleUpdateAssetIssuer(rec, req, "some-id")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid asset address in update, got %d", rec.Code)
	}
}
