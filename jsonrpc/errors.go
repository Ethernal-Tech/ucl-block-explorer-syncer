package jsonrpc

import "encoding/json"

// Error is a JSON-RPC 2.0 error (subset used by polygon-edge).
type Error interface {
	error
	ErrorCode() int
}

type invalidParamsError struct{ msg string }

func (e *invalidParamsError) Error() string  { return e.msg }
func (e *invalidParamsError) ErrorCode() int { return -32602 }
func NewInvalidParamsError(msg string) Error { return &invalidParamsError{msg: msg} }

type invalidRequestError struct{ msg string }

func (e *invalidRequestError) Error() string  { return e.msg }
func (e *invalidRequestError) ErrorCode() int { return -32600 }
func NewInvalidRequestError(msg string) Error { return &invalidRequestError{msg: msg} }

type methodNotFoundError struct{ msg string }

func (e *methodNotFoundError) Error() string  { return e.msg }
func (e *methodNotFoundError) ErrorCode() int { return -32601 }
func NewMethodNotFoundError(msg string) Error { return &methodNotFoundError{msg: msg} }

type internalError struct{ msg string }

func (e *internalError) Error() string  { return e.msg }
func (e *internalError) ErrorCode() int { return -32603 }
func NewInternalError(msg string) Error { return &internalError{msg: msg} }

// Request is a JSON-RPC request object.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}
