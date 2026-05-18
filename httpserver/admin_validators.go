package httpserver

import (
	"database/sql"
	"encoding/json"
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

	// Auth check
	if s.cfg.AdminAPISecret == "" {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "admin API disabled"})
		return
	}

	token := parseBearerToken(r)
	if token == "" || !constantTimeEqualString(token, s.cfg.AdminAPISecret) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
		return
	}

	if s.cfg.DB == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "database not configured"})
		return
	}

	// Extract address from path: /admin/v1/validators/0x...
	path := strings.TrimPrefix(r.URL.Path, "/admin/v1/validators/")
	if path == "" || path == r.URL.Path {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "address required in path"})
		return
	}

	addr := strings.TrimSpace(path)
	if !common.IsHexAddress(addr) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid address"})
		return
	}
	normalized := strings.ToLower(common.HexToAddress(addr).Hex())

	switch r.Method {
	case http.MethodPut, http.MethodPost:
		s.handleUpsertValidator(w, r, normalized)
	case http.MethodDelete:
		s.handleDeleteValidator(w, normalized)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
	}
}

func (s *Server) handleUpsertValidator(w http.ResponseWriter, r *http.Request, address string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxAdminJSONBody))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid body"})
		return
	}

	var req validatorMetadataRequest
	if err := json.Unmarshal(body, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON"})
		return
	}

	err = api_storage.UpsertValidatorMetadata(api_storage.ValidatorMetadata{
		Address:     address,
		Name:        strings.TrimSpace(req.Name),
		Institution: strings.TrimSpace(req.Institution),
		Region:      strings.TrimSpace(req.Region),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "database error"})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"address": address,
	})
}

func (s *Server) handleDeleteValidator(w http.ResponseWriter, address string) {
	err := api_storage.DeleteValidatorMetadata(address)
	if err == sql.ErrNoRows {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
		return
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "database error"})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"address": address,
	})
}
