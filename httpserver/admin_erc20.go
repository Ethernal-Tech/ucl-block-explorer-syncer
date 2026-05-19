package httpserver

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"

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

	// Check if already in watchlist — skip contract verification for updates
	var alreadyExists bool
	_ = s.cfg.DB.QueryRowContext(r.Context(),
		`SELECT EXISTS(SELECT 1 FROM chain.erc20_watchlist WHERE lower(address) = $1)`,
		normalized).Scan(&alreadyExists)

	if !alreadyExists {
		// Check entity_hour_participation — if found, it's an EOA
		var isEOA bool
		_ = s.cfg.DB.QueryRowContext(r.Context(),
			`SELECT EXISTS(SELECT 1 FROM chain.entity_hour_participation WHERE lower(address) = $1 LIMIT 1)`,
			normalized).Scan(&isEOA)
		if isEOA {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "address is an EOA, not a contract"})
			return
		}

		// Verify with eth_getCode
		if s.cfg.NodeRPC != "" {
			isContract, err := isContract(s.cfg.NodeRPC, normalized)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to verify contract address"})
				return
			}
			if !isContract {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "address is not an ERC-20 contract"})
				return
			}
		}
	}

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

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"address": normalized,
	})
}

func isContract(rpcURL, addr string) (bool, error) {
	client, err := rpc.Dial(rpcURL)
	if err != nil {
		return false, err
	}

	var code hexutil.Bytes

	if err := client.CallContext(context.TODO(),
		&code,
		"eth_getCode",
		addr,
		"latest"); err != nil {
		return false, fmt.Errorf("failed to get code: %w", err)
	}

	if len(code) == 0 {
		return false, nil // EOA, no code
	}

	return isERC20Bytecode(common.Bytes2Hex(code)), nil
}

func isERC20Bytecode(bytecode string) bool {
	// Standard ERC-20 function selectors
	selectors := []string{
		"18160ddd", // totalSupply()
		"70a08231", // balanceOf(address)
		"a9059cbb", // transfer(address,uint256)
		"dd62ed3e", // allowance(address,address)
		"095ea7b3", // approve(address,uint256)
		"23b872dd", // transferFrom(address,address,uint256)
	}

	for _, sel := range selectors {
		if !strings.Contains(bytecode, sel) {
			return false
		}
	}
	return true
}
