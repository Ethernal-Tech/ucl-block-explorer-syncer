package httpserver

import (
	"encoding/json"
	"net/http"
)

const (
	adminAPIDisabled = "admin API disabled"
	unauthorized     = "unathorized"
	dbNotConfigured  = "database not configured"
	methodNotAllowed = "method not allowed"
	invalidBody      = "invalid body"
	invalidJSON      = "invalid JSON"
	dbError          = "database error"
)

func writeError(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
