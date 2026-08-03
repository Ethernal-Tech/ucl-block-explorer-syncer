package httpserver

import (
	"encoding/json"
	"net/http"
)

// publicAPIError is the stable REST error envelope for /api/v1 routes.
type publicAPIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type publicAPIErrorBody struct {
	Error publicAPIError `json:"error"`
}

func writePublicError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(publicAPIErrorBody{
		Error: publicAPIError{
			Code:    code,
			Message: message,
		},
	})
}

func writePublicJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
