package httpserver

import (
	"encoding/json"
	"net/http"
)

const (
	adminAPIDisabled           = "admin API disabled"
	unauthorized               = "unathorized"
	dbNotConfigured            = "database not configured"
	methodNotAllowed           = "method not allowed"
	invalidBody                = "invalid body"
	invalidJSON                = "invalid JSON"
	dbError                    = "database error"
	jsonErrorKey               = "error"
	jsonAddressKey             = "address"
	latestBlockTag             = "latest"
	databaseErrorCode          = "database_error"
	invalidTransactionHashCode = "invalid_transaction_hash"
	invalidAddressCode         = "invalid_address"
	invalidBlockCode           = "invalid_block"
)

func writeError(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{jsonErrorKey: msg})
}
