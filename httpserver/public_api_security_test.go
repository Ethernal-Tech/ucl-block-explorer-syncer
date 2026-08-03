package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/explorer"
	"github.com/ethereum/go-ethereum/common"
)

const sqlInjectionPayload = "' OR 1=1; DROP TABLE chain.blocks; --"

type recordingBalanceReader struct {
	called bool
	err    error
}

func (r *recordingBalanceReader) BalanceAt(
	_ context.Context,
	_ common.Address,
	_ string,
) (*big.Int, error) {
	r.called = true

	return nil, r.err
}

func TestPublicAPI_RejectsInjectionPayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantCode string
	}{
		{
			name:     "transaction hash",
			path:     "/api/v1/transactions/" + url.PathEscape(sqlInjectionPayload),
			wantCode: "invalid_transaction_hash",
		},
		{
			name:     "oversized transaction hash",
			path:     "/api/v1/transactions/0x" + strings.Repeat("a", 8192),
			wantCode: "invalid_transaction_hash",
		},
		{
			name:     "balance address",
			path:     "/api/v1/addresses/" + url.PathEscape(sqlInjectionPayload) + "/balance",
			wantCode: "invalid_address",
		},
		{
			name:     "balance block",
			path:     "/api/v1/addresses/0x0000000000000000000000000000000000000001/balance?block=" + url.QueryEscape(sqlInjectionPayload),
			wantCode: "invalid_block",
		},
		{
			name:     "token address",
			path:     "/api/v1/tokens/" + url.PathEscape(sqlInjectionPayload) + "/transfers",
			wantCode: "invalid_address",
		},
		{
			name: "transfer address filter",
			path: "/api/v1/tokens/0x0000000000000000000000000000000000000001/transfers?address=" +
				url.QueryEscape(sqlInjectionPayload),
			wantCode: "invalid_address",
		},
		{
			name: "transfer from block",
			path: "/api/v1/tokens/0x0000000000000000000000000000000000000001/transfers?fromBlock=" +
				url.QueryEscape(sqlInjectionPayload),
			wantCode: "invalid_block",
		},
		{
			name: "transfer to block",
			path: "/api/v1/tokens/0x0000000000000000000000000000000000000001/transfers?toBlock=" +
				url.QueryEscape(sqlInjectionPayload),
			wantCode: "invalid_block",
		},
		{
			name: "transfer cursor",
			path: "/api/v1/tokens/0x0000000000000000000000000000000000000001/transfers?cursor=" +
				url.QueryEscape(sqlInjectionPayload),
			wantCode: "invalid_cursor",
		},
		{
			name:     "blocks page",
			path:     "/api/v1/blocks?page=" + url.QueryEscape(sqlInjectionPayload),
			wantCode: "invalid_page",
		},
		{
			name:     "blocks page size",
			path:     "/api/v1/blocks?pageSize=" + url.QueryEscape(sqlInjectionPayload),
			wantCode: "invalid_page_size",
		},
		{
			name:     "blocks boolean",
			path:     "/api/v1/blocks?onlyWithTransactions=" + url.QueryEscape(sqlInjectionPayload),
			wantCode: "invalid_query_parameter",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := New(explorer.NewExplorer(), Config{})
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			s.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status: got %d want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			var body publicAPIErrorBody
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if body.Error.Code != tc.wantCode {
				t.Fatalf("error code: got %q want %q", body.Error.Code, tc.wantCode)
			}
		})
	}
}

func TestPublicAPI_DoesNotExposeBackendErrors(t *testing.T) {
	t.Parallel()

	const sensitive = `pq: syntax error near "secret_table"; postgres://admin:password@db/internal`
	validHash := "0x" + strings.Repeat("a", 64)
	validAddress := "0x0000000000000000000000000000000000000001"

	tests := []struct {
		name       string
		path       string
		wantStatus int
		server     func() *Server
	}{
		{
			name:       "blocks",
			path:       "/api/v1/blocks",
			wantStatus: http.StatusServiceUnavailable,
			server: func() *Server {
				s := New(explorer.NewExplorer(), Config{})
				s.getBlockList = func(*api_storage.BlockListRequest) (interface{}, error) {
					return nil, errors.New(sensitive)
				}

				return s
			},
		},
		{
			name:       "transaction",
			path:       "/api/v1/transactions/" + validHash,
			wantStatus: http.StatusServiceUnavailable,
			server: func() *Server {
				s := New(explorer.NewExplorer(), Config{})
				s.getTransactionByHash = func(string) (*api_storage.TransactionListResponse, error) {
					return nil, errors.New(sensitive)
				}

				return s
			},
		},
		{
			name:       "balance",
			path:       "/api/v1/addresses/" + validAddress + "/balance",
			wantStatus: http.StatusBadGateway,
			server: func() *Server {
				return New(explorer.NewExplorer(), Config{
					BalanceReader: &recordingBalanceReader{err: errors.New(sensitive)},
				})
			},
		},
		{
			name:       "transfers",
			path:       "/api/v1/tokens/" + validAddress + "/transfers",
			wantStatus: http.StatusServiceUnavailable,
			server: func() *Server {
				s := New(explorer.NewExplorer(), Config{})
				s.getTokenTransfers = func(api_storage.TokenTransfersRequest) (*api_storage.TokenTransfersResponse, error) {
					return nil, errors.New(sensitive)
				}

				return s
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			tc.server().Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status: got %d want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), sensitive) ||
				strings.Contains(rec.Body.String(), "secret_table") ||
				strings.Contains(rec.Body.String(), "password") {
				t.Fatalf("response exposed backend details: %s", rec.Body.String())
			}
		})
	}
}
