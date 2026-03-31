package jsonrpc

import (
	"bytes"
	"encoding/json"
)

// ObjectError is the JSON-RPC error object.
type ObjectError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// SuccessResponse matches polygon-edge jsonrpc success shape.
type SuccessResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
}

// ErrorResponse matches polygon-edge jsonrpc error shape.
type ErrorResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Error   *ObjectError    `json:"error"`
}

// NewRPCResponse builds a response envelope (same semantics as polygon-edge codec).
func NewRPCResponse(id json.RawMessage, jsonrpcver string, reply []byte, err Error) []byte {
	if err == nil {
		out, marshalErr := json.Marshal(SuccessResponse{
			JSONRPC: jsonrpcver,
			ID:      id,
			Result:  reply,
		})
		if marshalErr != nil {
			return mustErr(nil, jsonrpcver, NewInternalError("marshal result"))
		}
		return out
	}

	oe := &ObjectError{
		Code:    err.ErrorCode(),
		Message: err.Error(),
	}
	out, marshalErr := json.Marshal(ErrorResponse{
		JSONRPC: jsonrpcver,
		ID:      id,
		Error:   oe,
	})
	if marshalErr != nil {
		return []byte(`{"jsonrpc":"2.0","error":{"code":-32603,"message":"internal"}}`)
	}
	return out
}

func mustErr(id json.RawMessage, ver string, err Error) []byte {
	b, _ := json.Marshal(ErrorResponse{
		JSONRPC: ver,
		ID:      id,
		Error:   &ObjectError{Code: err.ErrorCode(), Message: err.Error()},
	})
	return b
}

// NormalizeID parses id for re-marshaling (preserve string vs number).
func NormalizeID(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return []byte("null")
	}
	return raw
}

// ParseRequestID extracts id field for responses.
func ParseRequestID(body []byte) json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return []byte("null")
	}
	if id, ok := m["id"]; ok {
		return id
	}
	return []byte("null")
}

// IsBatchRequest returns true if body is a JSON array.
func IsBatchRequest(body []byte) bool {
	b := bytes.TrimLeft(body, " \t\r\n")
	return len(b) > 0 && b[0] == '['
}
