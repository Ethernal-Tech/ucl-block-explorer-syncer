package httpserver

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (s *Server) handleAdminLogin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	body, err := io.ReadAll(io.LimitReader(r.Body, maxAdminJSONBody))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid body"})
		return
	}

	var req loginRequest
	if err := json.Unmarshal(body, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON"})
		return
	}

	username := strings.TrimSpace(req.Username)
	if username == "" || req.Password == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "username and password required"})
		return
	}

	ok, err := api_storage.AuthenticateAdmin(username, req.Password)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "auth error"})
		return
	}
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid credentials"})
		return
	}

	if err := s.sessionManager.RenewToken(r.Context()); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "session error"})
		return
	}

	s.sessionManager.Put(r.Context(), "admin", true)
	s.sessionManager.Put(r.Context(), "username", username)

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":       true,
		"username": username,
	})
}

func (s *Server) handleAdminLogout(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if err := s.sessionManager.Destroy(r.Context()); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "session error"})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

// handleAdminSession lets the frontend check if the session is still valid.
func (s *Server) handleAdminSession(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if !s.sessionManager.GetBool(r.Context(), "admin") {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not logged in"})
		return
	}

	username := s.sessionManager.GetString(r.Context(), "username")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":       true,
		"username": username,
	})
}

func (s *Server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Bearer token auth (backwards compatible with curl/Postman)
		token := parseBearerToken(r)
		if token != "" && s.cfg.AdminAPISecret != "" && constantTimeEqualString(token, s.cfg.AdminAPISecret) {
			next(w, r)
			return
		}

		// Session cookie auth (browser)
		if s.sessionManager.GetBool(r.Context(), "admin") {
			next(w, r)
			return
		}

		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
	}
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
