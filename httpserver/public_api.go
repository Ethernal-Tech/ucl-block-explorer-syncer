package httpserver

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"strings"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	commonHelper "github.com/Ethernal-Tech/ucl-block-explorer-syncer/common"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/httpserver/publicapi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

const (
	defaultBlocksPageSize    = 25
	maxBlocksPageSize        = 100
	defaultTransfersPageSize = 50
	maxTransfersPageSize     = 100
)

// Ensure Server satisfies the generated public API contract.
var _ publicapi.ServerInterface = (*Server)(nil)

// GetBlocks implements publicapi.ServerInterface.
func (s *Server) GetBlocks(w http.ResponseWriter, r *http.Request, params publicapi.GetBlocksParams) {
	page := 1

	if params.Page != nil {
		if *params.Page < 1 {
			writePublicError(w, http.StatusBadRequest, "invalid_page", "page must be a positive integer")

			return
		}

		page = *params.Page
	}

	pageSize := defaultBlocksPageSize

	if params.PageSize != nil {
		if *params.PageSize < 1 || *params.PageSize > maxBlocksPageSize {
			writePublicError(w, http.StatusBadRequest, "invalid_page_size",
				"pageSize must be an integer between 1 and 100")

			return
		}

		pageSize = *params.PageSize
	}

	onlyWithTxn := false
	if params.OnlyWithTransactions != nil {
		onlyWithTxn = *params.OnlyWithTransactions
	}

	maxBlockNumber := ""
	if params.MaxBlockNumber != nil {
		maxBlockNumber = strings.TrimSpace(*params.MaxBlockNumber)
	}

	req := &api_storage.BlockListRequest{
		Page:           page,
		PageSize:       pageSize,
		MaxBlockNumber: maxBlockNumber,
		OnlyWithTxn:    onlyWithTxn,
	}

	getBlockList := s.getBlockList
	if getBlockList == nil {
		getBlockList = s.explorer.GetBlockList
	}

	out, err := getBlockList(req)
	if err != nil {
		writePublicError(w, http.StatusServiceUnavailable, databaseErrorCode, "failed to list blocks")

		return
	}

	resp, ok := out.(*api_storage.BlockListResponse)
	if !ok || resp == nil {
		writePublicError(w, http.StatusInternalServerError, "internal_error", "unexpected response type")

		return
	}

	if resp.Code != "200" {
		writePublicError(w, http.StatusServiceUnavailable, databaseErrorCode, resp.Message)

		return
	}

	list := make([]publicapi.Block, 0, len(resp.Data.List))
	for _, item := range resp.Data.List {
		list = append(list, publicapi.Block{
			BlockHash:   item.BlockHash,
			BlockNumber: item.BlockNumber,
			Nonce:       item.Nonce,
			Timestamp:   item.Timestamp,
			Txn:         item.Txn,
		})
	}

	writePublicJSON(w, http.StatusOK, publicapi.BlocksResponse{
		List:     list,
		Total:    resp.Data.Total,
		Page:     resp.Data.Page,
		PageSize: resp.Data.PageSize,
	})
}

// GetTransactionByHash implements publicapi.ServerInterface.
func (s *Server) GetTransactionByHash(
	w http.ResponseWriter,
	r *http.Request,
	hash publicapi.TransactionHash,
) {
	if !isValidTransactionHash(hash) {
		writePublicError(w, http.StatusBadRequest, invalidTransactionHashCode,
			"transaction hash must be a 0x-prefixed 32-byte hexadecimal value")

		return
	}

	getTransactionByHash := s.getTransactionByHash
	if getTransactionByHash == nil {
		getTransactionByHash = api_storage.GetTransactionByHash
	}

	resp, err := getTransactionByHash(hash)
	if err != nil {
		if resp != nil && resp.Code == "400" {
			writePublicError(w, http.StatusBadRequest, invalidTransactionHashCode, resp.Message)

			return
		}

		writePublicError(w, http.StatusServiceUnavailable, databaseErrorCode, "failed to get transaction")

		return
	}

	if resp.Code == "404" || len(resp.Data.List) == 0 {
		writePublicError(w, http.StatusNotFound, "transaction_not_found", "transaction not found")

		return
	}

	if resp.Code != "200" {
		writePublicError(w, http.StatusServiceUnavailable, databaseErrorCode, resp.Message)

		return
	}

	item := resp.Data.List[0]
	writePublicJSON(w, http.StatusOK, publicapi.Transaction{
		BlockNumber: item.BlockNumber,
		From:        item.From,
		Hash:        item.Hash,
		Id:          item.ID,
		To:          item.To,
		Timestamp:   item.Timestamp,
		Metadata: publicapi.TransactionMetadata{
			FunctionName: item.Metadata.FunctionName,
		},
		Data:         item.Data,
		IsDataAnchor: item.IsDataAnchor,
	})
}

// GetAddressBalance implements publicapi.ServerInterface.
func (s *Server) GetAddressBalance(
	w http.ResponseWriter,
	r *http.Request,
	address publicapi.Address,
	params publicapi.GetAddressBalanceParams,
) {
	normalized, err := commonHelper.NormalizeAddress(address)
	if err != nil {
		writePublicError(w, http.StatusBadRequest, invalidAddressCode, "address must be a valid hex address")

		return
	}

	blockParam := latestBlockTag
	if params.Block != nil && strings.TrimSpace(*params.Block) != "" {
		blockParam = strings.TrimSpace(*params.Block)
	}

	if _, err := toBalanceBlockArg(blockParam); err != nil {
		writePublicError(w, http.StatusBadRequest, invalidBlockCode,
			"block must be latest or a nonnegative decimal block number")

		return
	}

	if s.balanceReader == nil {
		writePublicError(w, http.StatusServiceUnavailable, "node_rpc_not_configured",
			"node RPC is not configured")

		return
	}

	bal, err := s.balanceReader.BalanceAt(r.Context(), common.HexToAddress(normalized), blockParam)
	if err != nil {
		writeBalanceRPCError(w, err)

		return
	}

	if bal == nil {
		bal = big.NewInt(0)
	}

	resolvedBlock := blockParam
	if strings.EqualFold(blockParam, latestBlockTag) {
		resolvedBlock = latestBlockTag
	}

	writePublicJSON(w, http.StatusOK, publicapi.AddressBalance{
		Address:    normalized,
		Block:      resolvedBlock,
		BalanceWei: bal.String(),
		BalanceHex: hexutil.EncodeBig(bal),
	})
}

// GetTokenTransfers implements publicapi.ServerInterface.
func (s *Server) GetTokenTransfers(
	w http.ResponseWriter,
	r *http.Request,
	tokenAddress publicapi.Address,
	params publicapi.GetTokenTransfersParams,
) {
	pageSize := defaultTransfersPageSize

	if params.PageSize != nil {
		if *params.PageSize < 1 || *params.PageSize > maxTransfersPageSize {
			writePublicError(w, http.StatusBadRequest, "invalid_page_size",
				"pageSize must be an integer between 1 and 100")

			return
		}

		pageSize = *params.PageSize
	}

	req := api_storage.TokenTransfersRequest{
		TokenAddress: tokenAddress,
		PageSize:     pageSize,
	}
	if params.Cursor != nil {
		req.Cursor = strings.TrimSpace(*params.Cursor)
	}

	if params.FromBlock != nil {
		req.FromBlock = strings.TrimSpace(*params.FromBlock)
	}

	if params.ToBlock != nil {
		req.ToBlock = strings.TrimSpace(*params.ToBlock)
	}

	if params.Address != nil {
		req.Address = strings.TrimSpace(*params.Address)
	}

	getTokenTransfers := s.getTokenTransfers
	if getTokenTransfers == nil {
		getTokenTransfers = api_storage.GetTokenTransfers
	}

	resp, err := getTokenTransfers(req)
	if err != nil {
		msg := err.Error()
		switch {
		case strings.Contains(msg, "invalid token address"):
			writePublicError(w, http.StatusBadRequest, invalidAddressCode, "tokenAddress must be a valid hex address")
		case strings.Contains(msg, "invalid address filter"):
			writePublicError(w, http.StatusBadRequest, invalidAddressCode, "address must be a valid hex address")
		case strings.Contains(msg, "invalid fromBlock"), strings.Contains(msg, "invalid toBlock"),
			strings.Contains(msg, "fromBlock must be"):
			writePublicError(w, http.StatusBadRequest, invalidBlockCode, msg)
		case strings.Contains(msg, "invalid cursor"):
			writePublicError(w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed")
		case api_storage.IsDBConnectionFailed(err):
			writePublicError(w, http.StatusServiceUnavailable, databaseErrorCode, "database not configured")
		default:
			writePublicError(w, http.StatusServiceUnavailable, databaseErrorCode, "failed to list token transfers")
		}

		return
	}

	list := make([]publicapi.TokenTransfer, 0, len(resp.List))
	for _, item := range resp.List {
		list = append(list, publicapi.TokenTransfer{
			TransactionHash: item.TransactionHash,
			LogIndex:        item.LogIndex,
			BlockNumber:     item.BlockNumber,
			TokenAddress:    item.TokenAddress,
			From:            item.From,
			To:              item.To,
			ValueRaw:        item.ValueRaw,
		})
	}

	out := publicapi.TokenTransfersResponse{
		List:     list,
		PageSize: resp.PageSize,
	}
	if resp.NextCursor != "" {
		cursor := resp.NextCursor
		out.NextCursor = &cursor
	}

	writePublicJSON(w, http.StatusOK, out)
}

// GetDailyCommitments implements publicapi.ServerInterface.
func (s *Server) GetDailyCommitments(
	w http.ResponseWriter,
	r *http.Request,
	params publicapi.GetDailyCommitmentsParams,
) {
	req := api_storage.DailyCommitmentsRequest{
		DayFrom: params.DayFrom,
		DayTo:   params.DayTo,
	}
	if params.FactoryAddress != nil {
		req.FactoryAddress = *params.FactoryAddress
	}

	if params.InstitutionId != nil {
		req.InstitutionID = *params.InstitutionId
	}

	if params.DataType != nil {
		req.DataType = *params.DataType
	}

	if params.Limit != nil {
		req.Limit = *params.Limit
	}

	if params.Offset != nil {
		req.Offset = *params.Offset
	}

	getDailyCommitments := s.getDailyCommitments
	if getDailyCommitments == nil {
		getDailyCommitments = api_storage.GetDailyCommitments
	}

	resp, err := getDailyCommitments(req)
	if err != nil {
		switch {
		case api_storage.IsDBConnectionFailed(err):
			writePublicError(w, http.StatusServiceUnavailable, databaseErrorCode, "database not configured")
		default:
			var validationErr *api_storage.DailyCommitmentsValidationError
			if !errors.As(err, &validationErr) {
				writePublicError(w, http.StatusServiceUnavailable, databaseErrorCode,
					"failed to list daily commitments")

				return
			}

			switch validationErr.Parameter {
			case "factory_address":
				writePublicError(w, http.StatusBadRequest, invalidAddressCode, validationErr.Error())
			case "institution_id", "data_type":
				writePublicError(w, http.StatusBadRequest, "invalid_bytes32", validationErr.Error())
			default:
				writePublicError(w, http.StatusBadRequest, "invalid_query_parameter", validationErr.Error())
			}
		}

		return
	}

	list := make([]publicapi.DailyCommitment, 0, len(resp.List))
	for _, item := range resp.List {
		list = append(list, publicapi.DailyCommitment{
			FactoryAddress:       item.FactoryAddress,
			DayTimestamp:         item.DayTimestamp,
			DataType:             item.DataType,
			InstitutionId:        item.InstitutionID,
			DailyContractAddress: item.DailyContractAddress,
			CommitmentCount:      item.CommitmentCount,
			DiscoveryBlock:       item.DiscoveryBlock,
		})
	}

	writePublicJSON(w, http.StatusOK, publicapi.DailyCommitmentsResponse{
		List:   list,
		Limit:  resp.Limit,
		Offset: resp.Offset,
	})
}

func writeBalanceRPCError(w http.ResponseWriter, err error) {
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "deadline exceeded") {
		writePublicError(w, http.StatusGatewayTimeout, "node_rpc_timeout", "node RPC request timed out")

		return
	}

	var rpcErr rpc.Error
	if errors.As(err, &rpcErr) {
		writePublicError(w, http.StatusBadGateway, "node_rpc_error", "node RPC returned an error")

		return
	}

	if strings.Contains(err.Error(), "unsupported block tag") ||
		strings.Contains(err.Error(), "invalid block number") {
		writePublicError(w, http.StatusBadRequest, invalidBlockCode,
			"block must be latest or a nonnegative decimal block number")

		return
	}

	writePublicError(w, http.StatusBadGateway, "node_rpc_error", "failed to fetch balance from node RPC")
}

func isValidTransactionHash(hash string) bool {
	hash = strings.TrimSpace(hash)
	if len(hash) != 66 || !strings.HasPrefix(hash, "0x") {
		return false
	}

	for _, c := range hash[2:] {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}

	return true
}

func handlePublicAPIParamError(w http.ResponseWriter, _ *http.Request, err error) {
	var invalidFormat *publicapi.InvalidParamFormatError
	if errors.As(err, &invalidFormat) {
		switch invalidFormat.ParamName {
		case "page":
			writePublicError(w, http.StatusBadRequest, "invalid_page", "page must be a positive integer")
		case "pageSize":
			writePublicError(w, http.StatusBadRequest, "invalid_page_size",
				"pageSize must be an integer between 1 and 100")
		case "onlyWithTransactions":
			writePublicError(w, http.StatusBadRequest, "invalid_query_parameter",
				"onlyWithTransactions must be a boolean")
		case "block", "fromBlock", "toBlock":
			writePublicError(w, http.StatusBadRequest, invalidBlockCode,
				"block must be latest or a nonnegative decimal block number")
		case "day_from", "day_to":
			writePublicError(w, http.StatusBadRequest, "invalid_query_parameter",
				invalidFormat.ParamName+" must be a nonnegative Unix timestamp")
		case jsonAddressKey, "tokenAddress", "factory_address":
			writePublicError(w, http.StatusBadRequest, invalidAddressCode, "address must be a valid hex address")
		case "institution_id", "data_type":
			writePublicError(w, http.StatusBadRequest, "invalid_bytes32",
				"value must be a 0x-prefixed 32-byte hexadecimal value")
		case "limit", "offset":
			writePublicError(w, http.StatusBadRequest, "invalid_query_parameter", invalidFormat.Error())
		case "hash":
			writePublicError(w, http.StatusBadRequest, invalidTransactionHashCode,
				"transaction hash must be a 0x-prefixed 32-byte hexadecimal value")
		case "cursor":
			writePublicError(w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed")
		default:
			writePublicError(w, http.StatusBadRequest, "invalid_query_parameter", invalidFormat.Error())
		}

		return
	}

	writePublicError(w, http.StatusBadRequest, "invalid_query_parameter", err.Error())
}
