package httpserver

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/common"
)

type assetIssuerRequest struct {
	Name    string   `json:"name"`
	Website string   `json:"website,omitempty"`
	Contact string   `json:"contact,omitempty"`
	Assets  []string `json:"assets,omitempty"`
	Region  string   `json:"region,omitempty"`
}

func (s *Server) handleAdminAssetIssuers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

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

	id := strings.TrimPrefix(r.URL.Path, "/admin/v1/asset-issuers/")
	if id == r.URL.Path {
		id = ""
	}

	switch r.Method {
	case http.MethodPost:
		if id != "" {
			writeError(w, http.StatusBadRequest, "POST does not take an ID in path")

			return
		}

		s.handleCreateAssetIssuer(w, r)
	case http.MethodPut:
		if id == "" {
			writeError(w, http.StatusBadRequest, "ID required in path")

			return
		}

		s.handleUpdateAssetIssuer(w, r, id)
	case http.MethodDelete:
		if id == "" {
			writeError(w, http.StatusBadRequest, "ID required in path")

			return
		}

		s.handleDeleteAssetIssuer(w, id)
	default:
		writeError(w, http.StatusMethodNotAllowed, methodNotAllowed)
	}
}

func (s *Server) handleCreateAssetIssuer(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxAdminJSONBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, invalidBody)

		return
	}

	var req assetIssuerRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, invalidJSON)

		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		writeError(w, http.StatusBadRequest, "name is required")

		return
	}

	normalizedAssets := make([]string, 0, len(req.Assets))
	for _, asset := range req.Assets {
		addr, err := common.NormalizeAddress(asset)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid asset address: %s", asset))
			return
		}
		normalizedAssets = append(normalizedAssets, addr)
	}

	id, err := api_storage.CreateAssetIssuer(api_storage.AssetIssuer{
		Name:    name,
		Website: strings.TrimSpace(req.Website),
		Contact: strings.TrimSpace(req.Contact),
		Assets:  normalizedAssets,
		Region:  strings.TrimSpace(req.Region),
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found in watchlist") ||
			strings.Contains(err.Error(), "already assigned") {
			writeError(w, http.StatusBadRequest, err.Error())

			return
		}

		writeError(w, http.StatusInternalServerError, dbError)

		return
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok": true,
		"id": id,
	})
}

func (s *Server) handleUpdateAssetIssuer(w http.ResponseWriter, r *http.Request, id string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxAdminJSONBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, invalidBody)

		return
	}

	var req assetIssuerRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, invalidJSON)

		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		writeError(w, http.StatusBadRequest, "name is required")

		return
	}

	normalizedAssets := make([]string, 0, len(req.Assets))
	for _, asset := range req.Assets {
		addr, err := common.NormalizeAddress(asset)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid asset address: %s", asset))
			return
		}
		normalizedAssets = append(normalizedAssets, addr)
	}

	err = api_storage.UpdateAssetIssuer(api_storage.AssetIssuer{
		ID:      id,
		Name:    name,
		Website: strings.TrimSpace(req.Website),
		Contact: strings.TrimSpace(req.Contact),
		Assets:  normalizedAssets,
		Region:  strings.TrimSpace(req.Region),
	})
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")

		return
	}

	if err != nil {
		if strings.Contains(err.Error(), "not found in watchlist") ||
			strings.Contains(err.Error(), "already assigned") {
			writeError(w, http.StatusBadRequest, err.Error())

			return
		}

		writeError(w, http.StatusInternalServerError, dbError)

		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok": true,
		"id": id,
	})
}

func (s *Server) handleDeleteAssetIssuer(w http.ResponseWriter, id string) {
	err := api_storage.DeleteAssetIssuer(id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")

		return
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, dbError)

		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok": true,
		"id": id,
	})
}
