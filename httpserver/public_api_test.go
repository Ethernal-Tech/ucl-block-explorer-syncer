package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/explorer"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/httpserver/publicapi"
	"github.com/ethereum/go-ethereum/common"
)

type mockBalanceReader struct {
	bal *big.Int
	err error
}

func (m *mockBalanceReader) BalanceAt(
	_ context.Context,
	_ common.Address,
	_ string,
) (*big.Int, error) {
	if m.err != nil {
		return nil, m.err
	}

	if m.bal == nil {
		return big.NewInt(0), nil
	}

	return new(big.Int).Set(m.bal), nil
}

func TestPublicAPI_RoutesNotSwallowedByCatchAll(t *testing.T) {
	t.Parallel()

	s := New(explorer.NewExplorer(), Config{
		BalanceReader: &mockBalanceReader{bal: big.NewInt(1)},
	})
	handler := s.Handler()

	tests := []struct {
		path       string
		wantStatus int
		wantCode   string
	}{
		{"/api/v1/blocks", http.StatusServiceUnavailable, databaseErrorCode},
		{"/api/v1/transactions/0x" + strings.Repeat("a", 64), http.StatusServiceUnavailable, databaseErrorCode},
		{"/api/v1/addresses/0x0000000000000000000000000000000000000001/balance", http.StatusOK, ""},
		{"/api/v1/tokens/0x0000000000000000000000000000000000000001/transfers", http.StatusServiceUnavailable, databaseErrorCode},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status: got %d want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}

			// Catch-all GET / returns chain metadata with "chain_id"; public errors use nested error.
			if strings.Contains(rec.Body.String(), `"chain_id"`) {
				t.Fatalf("route was swallowed by catch-all: %s", rec.Body.String())
			}

			if tc.wantCode != "" {
				var body publicAPIErrorBody
				if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
					t.Fatalf("decode: %v", err)
				}

				if body.Error.Code != tc.wantCode {
					t.Fatalf("error code: got %q want %q", body.Error.Code, tc.wantCode)
				}
			}
		})
	}
}

func TestGetTransactionByHash_InvalidHash(t *testing.T) {
	t.Parallel()

	s := &Server{explorer: explorer.NewExplorer()}

	tests := []string{
		"not-a-hash",
		"0x1234",
		"0x" + strings.Repeat("g", 64),
		strings.Repeat("a", 64),
	}

	for _, hash := range tests {
		hash := hash
		t.Run(hash, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/v1/transactions/"+hash, nil)

			s.GetTransactionByHash(rec, req, hash)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status: got %d want 400", rec.Code)
			}

			var body publicAPIErrorBody
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
				t.Fatalf("decode: %v", err)
			}

			if body.Error.Code != invalidTransactionHashCode {
				t.Fatalf("code: got %q", body.Error.Code)
			}
		})
	}
}

func TestGetTransactionByHash_IsDataAnchorSerialized(t *testing.T) {
	t.Parallel()

	hash := "0x" + strings.Repeat("ab", 32)
	s := New(explorer.NewExplorer(), Config{})
	s.getTransactionByHash = func(string) (*api_storage.TransactionListResponse, error) {
		return &api_storage.TransactionListResponse{
			Code: "200",
			Data: api_storage.TransactionListData{
				List: []api_storage.TransactionListItem{{
					BlockNumber: 123,
					From:        "0x1111111111111111111111111111111111111111",
					Hash:        hash,
					ID:          1,
					To:          "0x2222222222222222222222222222222222222222",
					Timestamp:   1710000000000,
					Metadata: api_storage.TransactionMetadata{
						FunctionName: "unknown",
					},
					Data:         "0xfe0e207b",
					IsDataAnchor: true,
				}},
				Total:    1,
				Page:     1,
				PageSize: 1,
			},
		}, nil
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/transactions/"+hash, nil)
	s.GetTransactionByHash(rec, req, hash)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200 body=%s", rec.Code, rec.Body.String())
	}

	var body publicapi.Transaction
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !body.IsDataAnchor {
		t.Fatalf("isDataAnchor: got false want true")
	}
}

func TestGetAddressBalance_ValidationAndNodeConfig(t *testing.T) {
	t.Parallel()

	t.Run("invalid address", func(t *testing.T) {
		t.Parallel()

		s := &Server{}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/addresses/not-an-address/balance", nil)

		s.GetAddressBalance(rec, req, "not-an-address", publicapi.GetAddressBalanceParams{})

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status: got %d", rec.Code)
		}

		var body publicAPIErrorBody

		_ = json.NewDecoder(rec.Body).Decode(&body)

		if body.Error.Code != invalidAddressCode {
			t.Fatalf("code: got %q", body.Error.Code)
		}
	})

	t.Run("invalid block", func(t *testing.T) {
		t.Parallel()

		s := &Server{balanceReader: &mockBalanceReader{bal: big.NewInt(0)}}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet,
			"/api/v1/addresses/0x0000000000000000000000000000000000000001/balance?block=pending", nil)
		block := "pending"

		s.GetAddressBalance(rec, req, "0x0000000000000000000000000000000000000001",
			publicapi.GetAddressBalanceParams{Block: &block})

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status: got %d body=%s", rec.Code, rec.Body.String())
		}

		var body publicAPIErrorBody

		_ = json.NewDecoder(rec.Body).Decode(&body)

		if body.Error.Code != invalidBlockCode {
			t.Fatalf("code: got %q", body.Error.Code)
		}
	})

	t.Run("node not configured", func(t *testing.T) {
		t.Parallel()

		s := &Server{}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet,
			"/api/v1/addresses/0x0000000000000000000000000000000000000001/balance", nil)

		s.GetAddressBalance(rec, req, "0x0000000000000000000000000000000000000001",
			publicapi.GetAddressBalanceParams{})

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status: got %d", rec.Code)
		}

		var body publicAPIErrorBody

		_ = json.NewDecoder(rec.Body).Decode(&body)

		if body.Error.Code != "node_rpc_not_configured" {
			t.Fatalf("code: got %q", body.Error.Code)
		}
	})

	t.Run("valid latest balance", func(t *testing.T) {
		t.Parallel()

		s := &Server{balanceReader: &mockBalanceReader{bal: big.NewInt(42)}}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet,
			"/api/v1/addresses/0x0000000000000000000000000000000000000001/balance", nil)

		s.GetAddressBalance(rec, req, "0x0000000000000000000000000000000000000001",
			publicapi.GetAddressBalanceParams{})

		if rec.Code != http.StatusOK {
			t.Fatalf("status: got %d body=%s", rec.Code, rec.Body.String())
		}

		var body publicapi.AddressBalance
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}

		if body.BalanceWei != "42" {
			t.Fatalf("balanceWei: got %q", body.BalanceWei)
		}

		if body.BalanceHex != "0x2a" {
			t.Fatalf("balanceHex: got %q", body.BalanceHex)
		}

		if body.Block != latestBlockTag {
			t.Fatalf("block: got %q", body.Block)
		}
	})

	t.Run("timeout maps to 504", func(t *testing.T) {
		t.Parallel()

		s := &Server{balanceReader: &mockBalanceReader{err: context.DeadlineExceeded}}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet,
			"/api/v1/addresses/0x0000000000000000000000000000000000000001/balance", nil)

		s.GetAddressBalance(rec, req, "0x0000000000000000000000000000000000000001",
			publicapi.GetAddressBalanceParams{})

		if rec.Code != http.StatusGatewayTimeout {
			t.Fatalf("status: got %d", rec.Code)
		}
	})

	t.Run("upstream error maps to 502", func(t *testing.T) {
		t.Parallel()

		s := &Server{balanceReader: &mockBalanceReader{err: errors.New("connection refused")}}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet,
			"/api/v1/addresses/0x0000000000000000000000000000000000000001/balance", nil)

		s.GetAddressBalance(rec, req, "0x0000000000000000000000000000000000000001",
			publicapi.GetAddressBalanceParams{})

		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status: got %d", rec.Code)
		}
	})
}

func TestGetBlocks_InvalidPageSize(t *testing.T) {
	t.Parallel()

	s := &Server{explorer: explorer.NewExplorer()}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/blocks?pageSize=999", nil)
	pageSize := 999

	s.GetBlocks(rec, req, publicapi.GetBlocksParams{PageSize: &pageSize})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d", rec.Code)
	}

	var body publicAPIErrorBody

	_ = json.NewDecoder(rec.Body).Decode(&body)

	if body.Error.Code != "invalid_page_size" {
		t.Fatalf("code: got %q", body.Error.Code)
	}
}

func TestGetTokenTransfers_InvalidTokenAddress(t *testing.T) {
	t.Parallel()

	s := &Server{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens/bad/transfers", nil)

	s.GetTokenTransfers(rec, req, "bad", publicapi.GetTokenTransfersParams{})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d body=%s", rec.Code, rec.Body.String())
	}

	var body publicAPIErrorBody

	_ = json.NewDecoder(rec.Body).Decode(&body)

	if body.Error.Code != invalidAddressCode {
		t.Fatalf("code: got %q", body.Error.Code)
	}
}

func TestToBalanceBlockArg(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    any
		wantErr bool
	}{
		{"", latestBlockTag, false},
		{latestBlockTag, latestBlockTag, false},
		{"LATEST", latestBlockTag, false},
		{"0", "0x0", false},
		{"255", "0xff", false},
		{"pending", nil, true},
		{"abc", nil, true},
		{"-1", nil, true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			got, err := toBalanceBlockArg(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != tc.want {
				t.Fatalf("got %#v want %#v", got, tc.want)
			}
		})
	}
}

func TestIsValidTransactionHash(t *testing.T) {
	t.Parallel()

	valid := "0x" + strings.Repeat("ab", 32)
	if !isValidTransactionHash(valid) {
		t.Fatal("expected valid")
	}

	if isValidTransactionHash("0x" + strings.Repeat("ab", 31)) {
		t.Fatal("expected invalid length")
	}
}
