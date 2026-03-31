package api_storage

// BlockListRequest Block list query request
type BlockListRequest struct {
	Page           int    `json:"page"`
	PageSize       int    `json:"pageSize"`
	MaxBlockNumber string `json:"maxBlockNumber"`
	OnlyWithTxn    bool   `json:"onlyWithTxn,omitempty"`
}

// BlockListResponse Block list query response
type BlockListResponse struct {
	Code    string        `json:"code"`
	Data    BlockListData `json:"data"`
	Message string        `json:"message"`
}

type BlockListData struct {
	List     []BlockListItem `json:"list"`
	Total    int64           `json:"total"`
	Page     int             `json:"page"`
	PageSize int             `json:"pageSize"`
}

type BlockListItem struct {
	BlockHash   string `json:"blockHash"`
	BlockNumber string `json:"blockNumber"`
	Nonce       string `json:"nonce"`
	Timestamp   int64  `json:"timestamp"`
	Txn         string `json:"txn"`
}

// BlockDetailRequest Block details query request
type BlockDetailRequest struct {
	BlockNumber string `json:"blockNumber"`
}

// BlockDetailResponse Block details query response
type BlockDetailResponse struct {
	Code    string          `json:"code"`
	Data    BlockDetailData `json:"data"`
	Message string          `json:"message"`
}

type BlockDetailData struct {
	BlockNumber string `json:"blockNumber"`
	BlockHash   string `json:"blockHash"`
	Txn         string `json:"txn"`
	Timestamp   int64  `json:"timestamp"`
	Nonce       string `json:"nonce"`
}

// LineDataRequest Line chart data query request
type LineDataRequest struct {
	Type string `json:"type"` // "day" | "hour"
}

// LineDataResponse Line chart data query response
type LineDataResponse struct {
	Code    string          `json:"code"`
	Data    []LineDataPoint `json:"data"`
	Message string          `json:"message"`
}

type LineDataPoint struct {
	Time  string `json:"time"`
	Count int64  `json:"count"`
}

// TransactionListRequest transaction list query request
type TransactionListRequest struct {
	Page        int    `json:"page"`
	PageSize    int    `json:"pageSize"`
	From        string `json:"from,omitempty"`
	To          string `json:"to,omitempty"`
	Hash        string `json:"hash,omitempty"`
	BlockNumber string `json:"blockNumber,omitempty"`
	StrictMode  bool   `json:"strictMode,omitempty"`
}

// TransactionListResponse transaction list query response
type TransactionListResponse struct {
	Code    string              `json:"code"`
	Data    TransactionListData `json:"data"`
	Message string              `json:"message"`
}

type TransactionListData struct {
	List     []TransactionListItem `json:"list"`
	Total    int64                 `json:"total"`
	Page     int                   `json:"page"`
	PageSize int                   `json:"pageSize"`
}

type TransactionListItem struct {
	BlockNumber int64               `json:"blockNumber"`
	From        string              `json:"from"`
	Hash        string              `json:"hash"`
	ID          int64               `json:"id"`
	To          string              `json:"to"`
	Timestamp   int64               `json:"timestamp"`
	Metadata    TransactionMetadata `json:"metadata"`
	Data        string              `json:"data"`
}

type TransactionMetadata struct {
	FunctionName string `json:"functionName"`
}

// method signature to function name mapping (whitelist for explorer queries)
var methodToFunctionName = map[string]string{
	"0xce23723f": "privateMint",
	"0xcc49f1b3": "privateSplitToken",
	"0x6a28376d": "privateBurn",
	"0xa0caa0ba": "privateTransfer",
	"0x40c10f19": "mint",
	"0x42966c68": "burn",
	"0xa9059cbb": "transfer",
	"0x97f1ae88": "convert2USDC",
	"0x7fa3845c": "convert2pUSDC",
}
