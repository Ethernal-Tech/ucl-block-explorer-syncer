package httpserver

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
)

type adminUserRequest struct {
	Username        string `json:"username"`
	Password        string `json:"password,omitempty"`
	CurrentPassword string `json:"currentPassword"`
}

func (s *Server) handleAdminUsers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	id := strings.TrimPrefix(r.URL.Path, "/admin/v1/users/")
	if id == r.URL.Path {
		id = ""
	}

	switch r.Method {
	case http.MethodGet:
		s.handleListAdminUsers(w)
	case http.MethodPost:
		s.handleCreateAdminUser(w, r)
	case http.MethodPut:
		if id == "" {
			writeError(w, http.StatusBadRequest, "ID required in path")

			return
		}

		s.handleUpdateAdminUser(w, r, id)
	case http.MethodDelete:
		if id == "" {
			writeError(w, http.StatusBadRequest, "ID required in path")

			return
		}

		s.handleDeleteAdminUser(w, r, id)
	default:
		writeError(w, http.StatusMethodNotAllowed, methodNotAllowed)
	}
}

func (s *Server) handleListAdminUsers(w http.ResponseWriter) {
	resp, err := api_storage.GetAdminUserList()
	if err != nil {
		writeError(w, http.StatusInternalServerError, dbError)

		return
	}

	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleCreateAdminUser(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxAdminJSONBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, invalidBody)

		return
	}

	var req adminUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, invalidJSON)

		return
	}

	username := strings.TrimSpace(req.Username)
	if username == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "username and password required")

		return
	}

	if err := api_storage.CrateAdminUser(username, req.Password); err != nil {
		if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "unique") {
			writeError(w, http.StatusConflict, "username already exists")

			return
		}

		writeError(w, http.StatusInternalServerError, dbError)

		return
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "username": username})
}

func (s *Server) handleUpdateAdminUser(w http.ResponseWriter, r *http.Request, id string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxAdminJSONBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, invalidBody)

		return
	}

	var req adminUserRequest

	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, invalidJSON)

		return
	}

	username := strings.TrimSpace(req.Username)
	if username == "" {
		writeError(w, http.StatusBadRequest, "username required")

		return
	}

	if req.CurrentPassword == "" {
		writeError(w, http.StatusBadRequest, "currentPasword required")

		return
	}

	err = api_storage.UpdateAdminUser(id, username, req.CurrentPassword, req.Password)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "user not found")

			return
		}

		if strings.Contains(err.Error(), "current password is incorrect") {
			writeError(w, http.StatusUnauthorized, "current password is incorrect")

			return
		}

		writeError(w, http.StatusInternalServerError, dbError)

		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "id": id})
}

func (s *Server) handleDeleteAdminUser(w http.ResponseWriter, r *http.Request, id string) {
	// Prevent deleting yourself
	currentUser := s.sessionManager.GetString(r.Context(), "username")

	var targetUsername string

	if resp, err := api_storage.GetAdminUserList(); err == nil {
		for _, u := range resp.Data {
			if u.ID == id {
				targetUsername = u.Username

				break
			}
		}
	}

	if currentUser != "" && currentUser == targetUsername {
		writeError(w, http.StatusBadRequest, "cannot delete yourself")

		return
	}

	err := api_storage.DeleteAdminUser(id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")

		return
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, dbError)

		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "id": id})
}
