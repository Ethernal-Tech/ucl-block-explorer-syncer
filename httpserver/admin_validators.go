package httpserver

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/ethereum/go-ethereum/common"
)

type validatorMetadataRequest struct {
	Name        string `json:"name"`
	Institution string `json:"institution"`
	Region      string `json:"region"`
}

func (s *Server) handleAdminValidators(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if s.cfg.DB == nil {
		writeError(w, http.StatusServiceUnavailable, dbNotConfigured)

		return
	}

	// Extract address from path: /admin/v1/validators/0x...
	path := strings.TrimPrefix(r.URL.Path, "/admin/v1/validators/")
	if path == "" || path == r.URL.Path {
		writeError(w, http.StatusBadRequest, "address required in path")

		return
	}

	addr := strings.TrimSpace(path)
	if !common.IsHexAddress(addr) {
		writeError(w, http.StatusBadRequest, "invalid address")

		return
	}

	normalized := strings.ToLower(common.HexToAddress(addr).Hex())

	switch r.Method {
	case http.MethodPut, http.MethodPost:
		s.handleUpsertValidator(w, r, normalized)
	case http.MethodDelete:
		s.handleDeleteValidator(w, normalized)
	default:
		writeError(w, http.StatusMethodNotAllowed, methodNotAllowed)
	}
}

func (s *Server) handleUpsertValidator(w http.ResponseWriter, r *http.Request, address string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxAdminJSONBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, invalidBody)

		return
	}

	var req validatorMetadataRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, invalidJSON)

		return
	}

	err = api_storage.UpsertValidatorMetadata(api_storage.ValidatorMetadata{
		Address:     address,
		Name:        strings.TrimSpace(req.Name),
		Institution: strings.TrimSpace(req.Institution),
		Region:      strings.TrimSpace(req.Region),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")

		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"address": address,
	})
}

func (s *Server) handleDeleteValidator(w http.ResponseWriter, address string) {
	err := api_storage.DeleteValidatorMetadata(address)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")

		return
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")

		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"address": address,
	})
}
