package httpserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleHealth(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	s.handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", body["status"])
	}

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json, got %q", ct)
	}
}

func TestHandleGetRequest(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{
		ChainName: "testnet",
		ChainID:   42,
		Version:   "1.2.3",
	}}

	rec := httptest.NewRecorder()
	s.handleGetRequest(rec)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp GetResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Name != "testnet" {
		t.Fatalf("name: got %q", resp.Name)
	}

	if resp.ChainID != 42 {
		t.Fatalf("chain_id: got %d", resp.ChainID)
	}

	if resp.Version != "1.2.3" {
		t.Fatalf("version: got %q", resp.Version)
	}
}

func TestHandleGetRequest_ZeroValues(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	s.handleGetRequest(rec)

	var resp GetResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Name != "" {
		t.Fatalf("expected empty name, got %q", resp.Name)
	}
}

func TestNew_DefaultVersion(t *testing.T) {
	t.Parallel()

	s := New(nil, Config{Version: ""})
	if s.cfg.Version != "0.0.1" {
		t.Fatalf("expected default version 0.0.1, got %q", s.cfg.Version)
	}
}

func TestNew_ExplicitVersion(t *testing.T) {
	t.Parallel()

	s := New(nil, Config{Version: "2.0.0"})
	if s.cfg.Version != "2.0.0" {
		t.Fatalf("expected 2.0.0, got %q", s.cfg.Version)
	}
}

func TestHandle_OptionsReturns200(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/", nil)

	s.handle(rec, req)

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type: got %q", ct)
	}
}

func TestHandle_UnsupportedMethodWritesError(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/", nil)

	s.handle(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "DELETE") && !strings.Contains(body, "not allowed") {
		t.Fatalf("expected method error in body, got %q", body)
	}
}

func TestHandle_GetReturnsChainInfo(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{ChainName: "mainnet", ChainID: 1, Version: "1.0.0"}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	s.handle(rec, req)

	var resp GetResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Name != "mainnet" {
		t.Fatalf("chain name: got %q", resp.Name)
	}
}

func TestHandleWS_NotImplemented(t *testing.T) {
	t.Parallel()

	s := &Server{cfg: Config{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)

	s.handleWS(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d", rec.Code)
	}
}

func TestMiddleware_SetsCORSHeaders(t *testing.T) {
	t.Parallel()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := middlewareFactory()(inner)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("CORS origin: got %q", got)
	}

	if got := rec.Header().Get("Access-Control-Allow-Methods"); got == "" {
		t.Fatal("missing Access-Control-Allow-Methods")
	}
}

func TestMiddleware_OptionsReturns204(t *testing.T) {
	t.Parallel()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // should not be reached
	})

	handler := middlewareFactory()(inner)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/", nil)

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for OPTIONS, got %d", rec.Code)
	}
}

func TestMiddleware_PassesThroughNonOptions(t *testing.T) {
	t.Parallel()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	handler := middlewareFactory()(inner)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTeapot {
		t.Fatalf("expected inner handler status 418, got %d", rec.Code)
	}
}

func TestHandler_NoDB_AdminRoutesMissing(t *testing.T) {
	t.Parallel()

	s := New(nil, Config{AdminAPISecret: "secret"})
	h := s.Handler()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/erc20/watchlist", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")

	h.ServeHTTP(rec, req) // must not panic
}
