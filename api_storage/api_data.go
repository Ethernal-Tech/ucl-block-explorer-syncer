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

// EntityDailyStatsRequest paginates rows with one aggregate count per bucket (active entities or onboarding EOAs).
type EntityDailyStatsRequest struct {
	Granularity string `json:"granularity,omitempty"` // "hour" | "day" | "month" (default "day")
	FromDay     string `json:"fromDay,omitempty"`     // YYYY-MM-DD
	ToDay       string `json:"toDay,omitempty"`
	FromUtc     string `json:"fromUtc,omitempty"` // RFC3339 UTC; use with toUtc
	ToUtc       string `json:"toUtc,omitempty"`
	Page        int    `json:"page"`
	PageSize    int    `json:"pageSize"`
}

// EntityDailyStatsResponse is the standard explorer envelope for per-day entity counts.
type EntityDailyStatsResponse struct {
	Code    string               `json:"code"`
	Data    EntityDailyStatsData `json:"data"`
	Message string               `json:"message"`
}

// EntityDailyStatsData is a paginated list of UTC days with counts.
type EntityDailyStatsData struct {
	List     []EntityDailyCountRow `json:"list"`
	Total    int64                 `json:"total"`
	Page     int                   `json:"page"`
	PageSize int                   `json:"pageSize"`
}

// EntityDailyCountRow is one time bucket: unique transacting addresses (active) or new EOAs (onboarding).
type EntityDailyCountRow struct {
	DayUtc    string `json:"dayUtc"`    // YYYY-MM-DD for day/month buckets; ISO8601 for hour when needed
	BucketUtc string `json:"bucketUtc"` // RFC3339 UTC start of bucket
	Count     int64  `json:"count"`
}

// Erc20DailyStatsRequest queries paginated aggregates for ERC-20 tokens (hourly facts, optional bucketing).
type Erc20DailyStatsRequest struct {
	Granularity  string `json:"granularity,omitempty"` // "hour" | "day" | "month" (default "day")
	TokenAddress string `json:"tokenAddress,omitempty"`
	FromDay      string `json:"fromDay,omitempty"` // YYYY-MM-DD
	ToDay        string `json:"toDay,omitempty"`
	FromUtc      string `json:"fromUtc,omitempty"`
	ToUtc        string `json:"toUtc,omitempty"`
	Page         int    `json:"page"`
	PageSize     int    `json:"pageSize"`
}

// Erc20DailyStatsResponse wraps daily stats rows in the standard explorer envelope.
type Erc20DailyStatsResponse struct {
	Code    string              `json:"code"`
	Data    Erc20DailyStatsData `json:"data"`
	Message string              `json:"message"`
}

// Erc20DailyStatsData paginated list of per-day aggregates.
type Erc20DailyStatsData struct {
	List     []Erc20DailyStatsRow `json:"list"`
	Total    int64                `json:"total"`
	Page     int                  `json:"page"`
	PageSize int                  `json:"pageSize"`
}

// Erc20DailyStatsRow is one bucket of aggregates for one token (raw uint256 sums as decimal strings).
type Erc20DailyStatsRow struct {
	TokenAddress      string `json:"tokenAddress"`
	DayUtc            string `json:"dayUtc"`    // legacy: date or ISO bucket label
	BucketUtc         string `json:"bucketUtc"` // RFC3339 UTC start of bucket
	TransferCount     int64  `json:"transferCount"`
	TransferVolumeRaw string `json:"transferVolumeRaw"`
	MintCount         int64  `json:"mintCount"`
	MintVolumeRaw     string `json:"mintVolumeRaw"`
	BurnCount         int64  `json:"burnCount"`
	BurnVolumeRaw     string `json:"burnVolumeRaw"`
}

// Erc20WatchlistResponse lists enabled watchlist tokens.
type Erc20WatchlistResponse struct {
	Code    string             `json:"code"`
	Data    Erc20WatchlistData `json:"data"`
	Message string             `json:"message"`
}

// Erc20WatchlistData holds the token list.
type Erc20WatchlistData struct {
	List []Erc20WatchlistItem `json:"list"`
}

// Erc20WatchlistItem is one row from chain.erc20_watchlist.
type Erc20WatchlistItem struct {
	Address  string `json:"address"`
	Symbol   string `json:"symbol,omitempty"`
	Decimals *int   `json:"decimals,omitempty"`
	Enabled  bool   `json:"enabled"`
}

// Erc20CirculationCumulativeRequest paginates cumulative circulation (all watchlisted tokens, human units).
type Erc20CirculationCumulativeRequest struct {
	Granularity string `json:"granularity,omitempty"` // "hour" | "day" | "month" (default "day")
	FromDay     string `json:"fromDay,omitempty"`
	ToDay       string `json:"toDay,omitempty"`
	FromUtc     string `json:"fromUtc,omitempty"`
	ToUtc       string `json:"toUtc,omitempty"`
	Page        int    `json:"page"`
	PageSize    int    `json:"pageSize"`
}

// Erc20CirculationCumulativeResponse is the standard explorer envelope for cumulative circulation points.
type Erc20CirculationCumulativeResponse struct {
	Code    string                         `json:"code"`
	Data    Erc20CirculationCumulativeData `json:"data"`
	Message string                         `json:"message"`
}

// Erc20CirculationCumulativeData is a paginated ascending list of UTC days with cumulative totals.
type Erc20CirculationCumulativeData struct {
	List     []Erc20CirculationCumulativeRow `json:"list"`
	Total    int64                             `json:"total"`
	Page     int                               `json:"page"`
	PageSize int                               `json:"pageSize"`
}

// Erc20CirculationCumulativeRow is end-of-bucket total in circulation (iterative clamp), decimal string.
type Erc20CirculationCumulativeRow struct {
	DayUtc    string `json:"dayUtc"`    // YYYY-MM-DD or ISO for chart compatibility
	BucketUtc string `json:"bucketUtc"` // RFC3339 UTC start of bucket
	Total     string `json:"total"`
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
