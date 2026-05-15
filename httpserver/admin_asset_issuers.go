package httpserver

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
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

	id := strings.TrimPrefix(r.URL.Path, "/admin/v1/asset-issuers/")
	if id == r.URL.Path {
		id = ""
	}

	switch r.Method {
	case http.MethodPost:
		if id != "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "POST does not take an ID in path"})
			return
		}
		s.handleCreateAssetIssuer(w, r)
	case http.MethodPut:
		if id == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "ID required in path"})
			return
		}
		s.handleUpdateAssetIssuer(w, r, id)
	case http.MethodDelete:
		if id == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "ID required in path"})
			return
		}
		s.handleDeleteAssetIssuer(w, id)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
	}
}

func (s *Server) handleCreateAssetIssuer(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxAdminJSONBody))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid body"})
		return
	}

	var req assetIssuerRequest
	if err := json.Unmarshal(body, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON"})
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "name is required"})
		return
	}

	id, err := api_storage.CreateAssetIssuer(api_storage.AssetIssuer{
		Name:    name,
		Website: strings.TrimSpace(req.Website),
		Contact: strings.TrimSpace(req.Contact),
		Assets:  req.Assets,
		Region:  strings.TrimSpace(req.Region),
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found in watchlist") ||
			strings.Contains(err.Error(), "already assigned") {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "database error"})
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
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid body"})
		return
	}

	var req assetIssuerRequest
	if err := json.Unmarshal(body, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON"})
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "name is required"})
		return
	}

	err = api_storage.UpdateAssetIssuer(api_storage.AssetIssuer{
		ID:      id,
		Name:    name,
		Website: strings.TrimSpace(req.Website),
		Contact: strings.TrimSpace(req.Contact),
		Assets:  req.Assets,
		Region:  strings.TrimSpace(req.Region),
	})
	if err == sql.ErrNoRows {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
		return
	}
	if err != nil {
		if strings.Contains(err.Error(), "not found in watchlist") ||
			strings.Contains(err.Error(), "already assigned") {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "database error"})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok": true,
		"id": id,
	})
}

func (s *Server) handleDeleteAssetIssuer(w http.ResponseWriter, id string) {
	err := api_storage.DeleteAssetIssuer(id)
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
		"ok": true,
		"id": id,
	})
}
