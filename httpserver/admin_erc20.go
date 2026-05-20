package httpserver

import (
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

const maxAdminJSONBody = 1 << 16

// erc20WatchlistRegisterRequest is the JSON body for POST /admin/v1/erc20/watchlist.
type erc20WatchlistRegisterRequest struct {
	Address  string `json:"address"`
	Symbol   string `json:"symbol,omitempty"`
	Decimals *int   `json:"decimals,omitempty"`
	Enabled  *bool  `json:"enabled,omitempty"`
}

func constantTimeEqualString(a, b string) bool {
	if len(a) != len(b) {
		return false
	}

	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func parseBearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")

	const prefix = "Bearer "
	if len(h) < len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return ""
	}

	return strings.TrimSpace(h[len(prefix):])
}

func (s *Server) handleAdminErc20Watchlist(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, methodNotAllowed)

		return
	}

	if s.cfg.AdminAPISecret == "" {
		writeError(w, http.StatusNotFound, adminAPIDisabled)

		return
	}

	token := parseBearerToken(r)
	if token == "" || !constantTimeEqualString(token, s.cfg.AdminAPISecret) {
		writeError(w, http.StatusUnauthorized, unauthorized)

		return
	}

	if s.cfg.DB == nil {
		writeError(w, http.StatusServiceUnavailable, dbNotConfigured)

		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxAdminJSONBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, invalidBody)

		return
	}

	var req erc20WatchlistRegisterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, invalidJSON)

		return
	}

	addr := strings.TrimSpace(req.Address)
	if !common.IsHexAddress(addr) {
		writeError(w, http.StatusBadRequest, "invalid address")

		return
	}

	normalized := strings.ToLower(common.HexToAddress(addr).Hex())

	symbol := strings.TrimSpace(req.Symbol)
	if len(symbol) > 32 {
		writeError(w, http.StatusBadRequest, "symbol too long (max32)")

		return
	}

	var dec sql.NullInt16

	if req.Decimals != nil {
		if *req.Decimals < -32768 || *req.Decimals > 32767 {
			writeError(w, http.StatusBadRequest, "invalid decimals")

			return
		}

		dec = sql.NullInt16{Int16: int16(*req.Decimals), Valid: true}
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	var sym interface{}
	if symbol != "" {
		sym = symbol
	}

	_, err = s.cfg.DB.ExecContext(r.Context(), `
		INSERT INTO chain.erc20_watchlist (address, symbol, decimals, enabled, updated_at)
		VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
		ON CONFLICT (address) DO UPDATE SET
			symbol = COALESCE(EXCLUDED.symbol, chain.erc20_watchlist.symbol),
			decimals = COALESCE(EXCLUDED.decimals, chain.erc20_watchlist.decimals),
			enabled = EXCLUDED.enabled,
			updated_at = CURRENT_TIMESTAMP
	`, normalized, sym, dec, enabled)
	if err != nil {
		writeError(w, http.StatusInternalServerError, dbError)

		return
	}

	//nolint:goconst
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"address": normalized,
	})
}
