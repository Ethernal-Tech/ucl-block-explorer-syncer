package httpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	commonHelper "github.com/Ethernal-Tech/ucl-block-explorer-syncer/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

type dataAnchorFactoryRequest struct {
	FactoryAddress string  `json:"factory_address"`
	StartBlock     *uint64 `json:"start_block"`
	Enabled        *bool   `json:"enabled,omitempty"`
}

func (s *Server) handleAdminDataAnchorFactories(w http.ResponseWriter, r *http.Request) {
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

	var req dataAnchorFactoryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, invalidJSON)

		return
	}

	if req.StartBlock == nil {
		writeError(w, http.StatusBadRequest, "start_block is required")

		return
	}

	if *req.StartBlock > uint64(1<<63-1) {
		writeError(w, http.StatusBadRequest, "start_block exceeds PostgreSQL BIGINT range")

		return
	}

	factoryAddress, err := commonHelper.NormalizeAddress(req.FactoryAddress)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid factory_address")

		return
	}

	var alreadyExists bool
	if err := s.cfg.DB.QueryRowContext(r.Context(), `
		SELECT EXISTS(
			SELECT 1 FROM chain.data_anchor_factory_watchlist WHERE factory_address = $1
		)
	`, factoryAddress).Scan(&alreadyExists); err != nil {
		log.Printf("admin data-anchor factory lookup error: %v", err)
		writeError(w, http.StatusInternalServerError, dbError)

		return
	}

	if !alreadyExists {
		if s.cfg.NodeRPC == "" {
			writeError(w, http.StatusServiceUnavailable, "node RPC is not configured")

			return
		}

		hasCode, err := hasContractCode(r.Context(), s.cfg.NodeRPC, factoryAddress)
		if err != nil {
			log.Printf("admin data-anchor contract verification error: %v", err)
			writeError(w, http.StatusBadGateway, "failed to verify factory contract address")

			return
		}

		if !hasCode {
			writeError(w, http.StatusBadRequest, "factory_address has no deployed contract bytecode")

			return
		}
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	var (
		startBlock    uint64
		nextBlock     uint64
		storedEnabled bool
	)

	err = s.cfg.DB.QueryRowContext(r.Context(), `
		INSERT INTO chain.data_anchor_factory_watchlist (
			factory_address, start_block, next_block, enabled
		)
		VALUES ($1, $2, $2, $3)
		ON CONFLICT (factory_address) DO UPDATE SET
			start_block = CASE
				WHEN chain.data_anchor_factory_watchlist.next_block =
					chain.data_anchor_factory_watchlist.start_block
				THEN EXCLUDED.start_block
				ELSE chain.data_anchor_factory_watchlist.start_block
			END,
			next_block = CASE
				WHEN chain.data_anchor_factory_watchlist.next_block =
					chain.data_anchor_factory_watchlist.start_block
				THEN EXCLUDED.start_block
				ELSE chain.data_anchor_factory_watchlist.next_block
			END,
			enabled = EXCLUDED.enabled,
			updated_at = CURRENT_TIMESTAMP
		WHERE EXCLUDED.start_block = chain.data_anchor_factory_watchlist.start_block
			OR chain.data_anchor_factory_watchlist.next_block =
				chain.data_anchor_factory_watchlist.start_block
		RETURNING start_block, next_block, enabled
	`, factoryAddress, *req.StartBlock, enabled).Scan(&startBlock, &nextBlock, &storedEnabled)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusConflict, "start_block cannot be changed after processing has begun")

		return
	}

	if err != nil {
		log.Printf("admin data-anchor factory upsert error: %v", err)
		writeError(w, http.StatusInternalServerError, dbError)

		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":              true,
		"factory_address": factoryAddress,
		"start_block":     startBlock,
		"next_block":      nextBlock,
		"enabled":         storedEnabled,
	})
}

func hasContractCode(ctx context.Context, rpcURL, address string) (bool, error) {
	client, err := rpc.DialContext(ctx, rpcURL)
	if err != nil {
		return false, fmt.Errorf("dial node RPC: %w", err)
	}
	defer client.Close()

	var code hexutil.Bytes
	if err := client.CallContext(ctx, &code, "eth_getCode", address, latestBlockTag); err != nil {
		return false, fmt.Errorf("get contract code: %w", err)
	}

	return len(code) > 0, nil
}
