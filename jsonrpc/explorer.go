package jsonrpc

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/explorer"
)

// ExplorerHandler dispatches explorer_* JSON-RPC methods (polygon-edge compatible).
type ExplorerHandler struct {
	Explorer *explorer.Explorer
}

// Dispatch executes one explorer JSON-RPC method and returns JSON bytes for the `result` field.
func (h *ExplorerHandler) Dispatch(method string, params json.RawMessage) ([]byte, Error) {
	if h.Explorer == nil {
		return nil, NewInternalError("explorer not configured")
	}

	if !strings.HasPrefix(method, "explorer_") {
		return nil, NewMethodNotFoundError(method)
	}

	var out interface{}
	var err error

	switch method {
	case "explorer_getBlockList":
		var req *api_storage.BlockListRequest
		parseOptionalObject(params, &req)
		out, err = h.Explorer.GetBlockList(req)
	case "explorer_getBlockDetail":
		req := &api_storage.BlockDetailRequest{}
		parseFirstObject(params, req)
		out, err = h.Explorer.GetBlockDetail(req)
	case "explorer_getLineData":
		req := &api_storage.LineDataRequest{}
		parseFirstObject(params, req)
		out, err = h.Explorer.GetLineData(req)
	case "explorer_getTransactionList":
		var req *api_storage.TransactionListRequest
		parseOptionalObject(params, &req)
		out, err = h.Explorer.GetTransactionList(req)
	case "explorer_getTransactionByHash":
		hash, perr := parseStringParam(params)
		if perr != nil {
			return nil, perr
		}
		out, err = h.Explorer.GetTransactionByHash(hash)
	case "explorer_getBlockTransactionCount":
		blockNumber, perr := parseStringParam(params)
		if perr != nil {
			return nil, perr
		}
		out, err = h.Explorer.GetBlockTransactionCount(blockNumber)
	default:
		return nil, NewMethodNotFoundError(method)
	}

	if err != nil {
		return nil, NewInternalError(err.Error())
	}

	raw, mErr := json.Marshal(out)
	if mErr != nil {
		return nil, NewInternalError("marshal result")
	}
	return raw, nil
}

// parseOptionalObject sets *ptr to nil when params empty, else unmarshals first array element into a new T.
func parseOptionalObject[T any](params json.RawMessage, ptr **T) {
	if len(params) == 0 || string(params) == "null" {
		*ptr = nil
		return
	}
	var arr []json.RawMessage
	if json.Unmarshal(params, &arr) != nil || len(arr) == 0 {
		*ptr = nil
		return
	}
	v := new(T)
	if json.Unmarshal(arr[0], v) != nil {
		*ptr = nil
		return
	}
	*ptr = v
}

func parseFirstObject(params json.RawMessage, dst interface{}) {
	if len(params) == 0 || string(params) == "null" {
		return
	}
	var arr []json.RawMessage
	if json.Unmarshal(params, &arr) != nil || len(arr) == 0 {
		return
	}
	_ = json.Unmarshal(arr[0], dst)
}

func parseStringParam(params json.RawMessage) (string, Error) {
	if len(params) == 0 || string(params) == "null" {
		return "", nil
	}
	var arr []interface{}
	if err := json.Unmarshal(params, &arr); err != nil {
		return "", NewInvalidParamsError("Invalid Params")
	}
	if len(arr) == 0 {
		return "", nil
	}
	s, ok := arr[0].(string)
	if !ok {
		return "", NewInvalidParamsError("Invalid Params")
	}
	return s, nil
}

// HandleBody processes a single JSON-RPC object or batch array.
func HandleBody(h *ExplorerHandler, body []byte) ([]byte, error) {
	b := bytes.TrimSpace(body)
	if len(b) == 0 {
		return NewRPCResponse(nil, "2.0", nil, NewInvalidRequestError("Invalid json request")), nil
	}

	if b[0] == '[' {
		var reqs []Request
		if err := json.Unmarshal(b, &reqs); err != nil {
			return NewRPCResponse(nil, "2.0", nil, NewInvalidRequestError("Invalid json request")), nil
		}
		var parts [][]byte
		for _, req := range reqs {
			parts = append(parts, handleOne(h, req))
		}
		return bytesJoin(parts), nil
	}

	var req Request
	if err := json.Unmarshal(b, &req); err != nil {
		return NewRPCResponse(nil, "2.0", nil, NewInvalidRequestError("Invalid json request")), nil
	}
	if req.Method == "" {
		id := req.ID
		if len(id) == 0 {
			id = []byte("null")
		}
		return NewRPCResponse(id, "2.0", nil, NewInvalidRequestError("Invalid json request")), nil
	}

	return handleOne(h, req), nil
}

func handleOne(h *ExplorerHandler, req Request) []byte {
	id := req.ID
	if len(id) == 0 {
		id = []byte("null")
	}

	res, rpcErr := h.Dispatch(req.Method, req.Params)
	if rpcErr != nil {
		return NewRPCResponse(id, "2.0", nil, rpcErr)
	}

	return NewRPCResponse(id, "2.0", res, nil)
}

func bytesJoin(parts [][]byte) []byte {
	if len(parts) == 0 {
		return []byte("[]")
	}
	var b []byte
	b = append(b, '[')
	for i, p := range parts {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, p...)
	}
	b = append(b, ']')
	return b
}
