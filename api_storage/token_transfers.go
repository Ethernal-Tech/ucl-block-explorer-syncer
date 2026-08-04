package api_storage

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	commonHelper "github.com/Ethernal-Tech/ucl-block-explorer-syncer/common"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/helper"
	"github.com/ethereum/go-ethereum/common"
	"github.com/lib/pq"
)

// TokenTransfersRequest lists ERC-20 Transfer logs for one token contract.
type TokenTransfersRequest struct {
	TokenAddress string
	Cursor       string
	PageSize     int
	FromBlock    string
	ToBlock      string
	Address      string // optional sender-or-recipient filter
}

// TokenTransferItem is one decoded ERC-20 Transfer event.
type TokenTransferItem struct {
	TransactionHash string `json:"transactionHash"`
	LogIndex        int    `json:"logIndex"`
	BlockNumber     int64  `json:"blockNumber"`
	TokenAddress    string `json:"tokenAddress"`
	From            string `json:"from"`
	To              string `json:"to"`
	ValueRaw        string `json:"valueRaw"`
}

// TokenTransfersResponse is the REST payload for token transfer listing.
type TokenTransfersResponse struct {
	List       []TokenTransferItem `json:"list"`
	NextCursor string              `json:"nextCursor,omitempty"`
	PageSize   int                 `json:"pageSize"`
}

type tokenTransferCursor struct {
	BlockNumber int64
	LogIndex    int
	TxHash      string
}

// GetTokenTransfers returns ERC-20 Transfer logs for a token with cursor pagination.
func GetTokenTransfers(req TokenTransfersRequest) (*TokenTransfersResponse, error) {
	req.PageSize = clampTokenTransfersPageSize(req.PageSize)

	tokenAddr, err := commonHelper.NormalizeAddress(req.TokenAddress)
	if err != nil {
		return nil, fmt.Errorf("invalid token address: %w", err)
	}

	var walletFilter string

	if strings.TrimSpace(req.Address) != "" {
		wallet, err := commonHelper.NormalizeAddress(req.Address)
		if err != nil {
			return nil, fmt.Errorf("invalid address filter: %w", err)
		}

		walletFilter = wallet
	}

	var fromBlock *int64

	if strings.TrimSpace(req.FromBlock) != "" {
		bn, ok := validBlockNumberString(req.FromBlock)
		if !ok {
			return nil, fmt.Errorf("invalid fromBlock")
		}

		n, _ := strconv.ParseInt(bn, 10, 64)
		fromBlock = &n
	}

	var toBlock *int64

	if strings.TrimSpace(req.ToBlock) != "" {
		bn, ok := validBlockNumberString(req.ToBlock)
		if !ok {
			return nil, fmt.Errorf("invalid toBlock")
		}

		n, _ := strconv.ParseInt(bn, 10, 64)
		toBlock = &n
	}

	if fromBlock != nil && toBlock != nil && *fromBlock > *toBlock {
		return nil, fmt.Errorf("fromBlock must be <= toBlock")
	}

	var cursor *tokenTransferCursor

	if strings.TrimSpace(req.Cursor) != "" {
		c, err := decodeTokenTransferCursor(req.Cursor)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}

		cursor = &c
	}

	conn := getDB()
	if conn == nil {
		log.Printf("api_storage: database not configured")

		return nil, errDBConnectionFailed
	}

	transferTopic := strings.ToLower(helper.TransferTopic.Hex())
	tokenLower := strings.ToLower(tokenAddr)

	query := `
		SELECT tx_hash, log_index, block_number, address, topics, data
		FROM chain.transaction_logs
		WHERE LOWER(address) = $1
		AND LOWER(topics[1]) = $2
	`
	args := []any{tokenLower, transferTopic}
	argN := 3

	if fromBlock != nil {
		query += fmt.Sprintf(" AND block_number >= $%d", argN)

		args = append(args, *fromBlock)
		argN++
	}

	if toBlock != nil {
		query += fmt.Sprintf(" AND block_number <= $%d", argN)

		args = append(args, *toBlock)
		argN++
	}

	if walletFilter != "" {
		padded := strings.ToLower(common.BytesToHash(common.HexToAddress(walletFilter).Bytes()).Hex())
		query += fmt.Sprintf(" AND (LOWER(topics[2]) = $%d OR LOWER(topics[3]) = $%d)", argN, argN)

		args = append(args, padded)
		argN++
	}

	if cursor != nil {
		query += fmt.Sprintf(`
			AND (
				block_number < $%d
				OR (block_number = $%d AND log_index < $%d)
				OR (block_number = $%d AND log_index = $%d AND tx_hash < $%d)
			)`, argN, argN, argN+1, argN, argN+1, argN+2)

		args = append(args, cursor.BlockNumber, cursor.LogIndex, strings.ToLower(cursor.TxHash))
		argN += 3
	}

	query += fmt.Sprintf(`
		ORDER BY block_number DESC, log_index DESC, tx_hash DESC
		LIMIT $%d
	`, argN)

	args = append(args, req.PageSize+1)

	rows, err := conn.Query(query, args...)
	if err != nil {
		log.Printf("api_storage: token transfers query: %v", err)

		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	items := make([]TokenTransferItem, 0, req.PageSize+1)

	for rows.Next() {
		var (
			txHash      string
			logIndex    int
			blockNumber int64
			address     string
			topics      pq.StringArray
			data        string
		)

		if err := rows.Scan(&txHash, &logIndex, &blockNumber, &address, &topics, &data); err != nil {
			log.Printf("api_storage: scan token transfer: %v", err)

			continue
		}

		from, to, value, ok := helper.DecodeTransferLog([]string(topics), data)
		if !ok {
			continue
		}

		items = append(items, TokenTransferItem{
			TransactionHash: txHash,
			LogIndex:        logIndex,
			BlockNumber:     blockNumber,
			TokenAddress:    common.HexToAddress(address).Hex(),
			From:            from.Hex(),
			To:              to.Hex(),
			ValueRaw:        value.String(),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	resp := &TokenTransfersResponse{
		List:     items,
		PageSize: req.PageSize,
	}

	if len(items) > req.PageSize {
		resp.List = items[:req.PageSize]
		last := resp.List[len(resp.List)-1]
		resp.NextCursor = encodeTokenTransferCursor(tokenTransferCursor{
			BlockNumber: last.BlockNumber,
			LogIndex:    last.LogIndex,
			TxHash:      last.TransactionHash,
		})
	}

	return resp, nil
}

func encodeTokenTransferCursor(c tokenTransferCursor) string {
	raw := fmt.Sprintf("%d|%d|%s", c.BlockNumber, c.LogIndex, strings.ToLower(c.TxHash))

	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeTokenTransferCursor(s string) (tokenTransferCursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return tokenTransferCursor{}, errors.New("malformed cursor encoding")
	}

	parts := strings.Split(string(b), "|")
	if len(parts) != 3 {
		return tokenTransferCursor{}, errors.New("malformed cursor payload")
	}

	blockNumber, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || blockNumber < 0 {
		return tokenTransferCursor{}, errors.New("malformed cursor block")
	}

	logIndex, err := strconv.Atoi(parts[1])
	if err != nil || logIndex < 0 {
		return tokenTransferCursor{}, errors.New("malformed cursor log index")
	}

	txHash := strings.ToLower(strings.TrimSpace(parts[2]))
	if !isValidTxHash(txHash) {
		return tokenTransferCursor{}, errors.New("malformed cursor tx hash")
	}

	return tokenTransferCursor{
		BlockNumber: blockNumber,
		LogIndex:    logIndex,
		TxHash:      txHash,
	}, nil
}

func isValidTxHash(hash string) bool {
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
