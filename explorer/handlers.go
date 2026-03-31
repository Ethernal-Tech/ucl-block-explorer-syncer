package explorer

import (
	"log"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
)

// Explorer implements the same JSON-RPC surface as polygon-edge jsonrpc/explorer_endpoint.go.
type Explorer struct {
	Logger func(string, ...any)
}

// NewExplorer creates an Explorer handler.
func NewExplorer() *Explorer {
	return &Explorer{
		Logger: func(format string, args ...any) {
			log.Printf(format, args...)
		},
	}
}

func (e *Explorer) logf(format string, args ...any) {
	if e.Logger != nil {
		e.Logger(format, args...)
	}
}

// GetBlockList returns a paginated list of blocks with transaction counts.
func (e *Explorer) GetBlockList(req *api_storage.BlockListRequest) (interface{}, error) {
	if req == nil {
		req = &api_storage.BlockListRequest{
			Page:     1,
			PageSize: 10,
		}
	}

	response, err := api_storage.GetBlockList(*req)
	if err != nil {
		e.logf("failed to get block list: %v", err)
		return nil, err
	}

	return response, nil
}

// GetBlockDetail returns detailed information about a specific block.
func (e *Explorer) GetBlockDetail(req *api_storage.BlockDetailRequest) (interface{}, error) {
	if req == nil || req.BlockNumber == "" {
		return &api_storage.BlockDetailResponse{
			Code:    "400",
			Message: "Block number is required",
		}, nil
	}

	response, err := api_storage.GetBlockDetail(*req)
	if err != nil {
		e.logf("failed to get block detail: %v", err)
		return nil, err
	}

	return response, nil
}

// GetLineData returns time series data for transaction charts.
func (e *Explorer) GetLineData(req *api_storage.LineDataRequest) (interface{}, error) {
	if req == nil {
		req = &api_storage.LineDataRequest{
			Type: "day",
		}
	}

	if req.Type != "day" && req.Type != "hour" {
		req.Type = "day"
	}

	response, err := api_storage.GetLineData(*req)
	if err != nil {
		e.logf("failed to get line data: %v", err)
		return nil, err
	}

	return response, nil
}

// GetTransactionList returns a paginated list of transactions with optional filters.
func (e *Explorer) GetTransactionList(req *api_storage.TransactionListRequest) (interface{}, error) {
	if req == nil {
		req = &api_storage.TransactionListRequest{
			Page:     1,
			PageSize: 100,
		}
	}

	response, err := api_storage.GetTransactionList(*req)
	if err != nil {
		e.logf("failed to get transaction list: %v", err)
		return nil, err
	}

	return response, nil
}

// GetTransactionByHash returns a single transaction by its hash.
func (e *Explorer) GetTransactionByHash(hash string) (interface{}, error) {
	if hash == "" {
		return &api_storage.TransactionListResponse{
			Code:    "400",
			Message: "Transaction hash is required",
		}, nil
	}

	response, err := api_storage.GetTransactionByHash(hash)
	if err != nil {
		e.logf("failed to get transaction by hash: %v", err)
		return nil, err
	}

	if response.Code == "200" && len(response.Data.List) > 0 {
		return response.Data.List[0], nil
	}

	return nil, nil
}

// GetBlockTransactionCount returns the number of whitelist transactions in a block.
func (e *Explorer) GetBlockTransactionCount(blockNumber string) (interface{}, error) {
	if blockNumber == "" {
		return map[string]interface{}{
			"code":    "400",
			"message": "Block number is required",
		}, nil
	}

	req := api_storage.BlockDetailRequest{
		BlockNumber: blockNumber,
	}

	response, err := api_storage.GetBlockDetail(req)
	if err != nil {
		e.logf("failed to get block transaction count: %v", err)
		return nil, err
	}

	if response.Code == "200" {
		return map[string]interface{}{
			"blockNumber": response.Data.BlockNumber,
			"txnCount":    response.Data.Txn,
		}, nil
	}

	return response, nil
}
