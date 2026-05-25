package httpserver

import (
	"encoding/json"
	"net/http"
)

const (
	dbNotConfigured  = "database not configured"
	methodNotAllowed = "method not allowed"
	invalidBody      = "invalid body"
	invalidJSON      = "invalid JSON"
	dbError          = "database error"
	secretStr        = "secret"
)

func writeError(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
