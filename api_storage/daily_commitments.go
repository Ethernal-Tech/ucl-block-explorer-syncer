package api_storage

import (
	"fmt"
	"log"
	"strings"

	commonHelper "github.com/Ethernal-Tech/ucl-block-explorer-syncer/common"
	"github.com/ethereum/go-ethereum/common"
)

const (
	DefaultDailyCommitmentsLimit = 50
	MaxDailyCommitmentsLimit     = 100
)

type DailyCommitmentsRequest struct {
	FactoryAddress string
	DayFrom        *int64
	DayTo          *int64
	InstitutionID  string
	DataType       string
	Limit          int
	Offset         int
}

type DailyCommitmentItem struct {
	FactoryAddress       string `json:"factory_address"`
	DayTimestamp         int64  `json:"day_timestamp"`
	DataType             string `json:"data_type"`
	InstitutionID        string `json:"institution_id"`
	DailyContractAddress string `json:"daily_contract_address"`
	CommitmentCount      int64  `json:"commitment_count"`
	DiscoveryBlock       int64  `json:"discovery_block"`
}

type DailyCommitmentsResponse struct {
	List   []DailyCommitmentItem `json:"list"`
	Limit  int                   `json:"limit"`
	Offset int                   `json:"offset"`
}

type DailyCommitmentsValidationError struct {
	Parameter string
	Err       error
}

func (e *DailyCommitmentsValidationError) Error() string {
	return e.Err.Error()
}

func (e *DailyCommitmentsValidationError) Unwrap() error {
	return e.Err
}

func dailyCommitmentsValidationError(parameter, format string, args ...any) error {
	return &DailyCommitmentsValidationError{
		Parameter: parameter,
		Err:       fmt.Errorf(format, args...),
	}
}

func GetDailyCommitments(req DailyCommitmentsRequest) (*DailyCommitmentsResponse, error) {
	if req.Limit == 0 {
		req.Limit = DefaultDailyCommitmentsLimit
	}

	if req.Limit < 1 || req.Limit > MaxDailyCommitmentsLimit {
		return nil, dailyCommitmentsValidationError(
			"limit", "limit must be between 1 and %d", MaxDailyCommitmentsLimit)
	}

	if req.Offset < 0 {
		return nil, dailyCommitmentsValidationError("offset", "offset must be nonnegative")
	}

	if req.DayFrom != nil && *req.DayFrom < 0 {
		return nil, dailyCommitmentsValidationError("day_from", "day_from must be nonnegative")
	}

	if req.DayTo != nil && *req.DayTo < 0 {
		return nil, dailyCommitmentsValidationError("day_to", "day_to must be nonnegative")
	}

	if req.DayFrom != nil && req.DayTo != nil && *req.DayFrom > *req.DayTo {
		return nil, dailyCommitmentsValidationError(
			"day_from", "day_from must be less than or equal to day_to")
	}

	where := []string{"TRUE"}
	args := make([]any, 0, 7)

	if strings.TrimSpace(req.FactoryAddress) != "" {
		address, err := commonHelper.NormalizeAddress(req.FactoryAddress)
		if err != nil {
			return nil, dailyCommitmentsValidationError(
				"factory_address", "invalid factory_address: %v", err)
		}

		args = append(args, address)
		where = append(where, fmt.Sprintf("factory_address = $%d", len(args)))
	}

	if req.DayFrom != nil {
		args = append(args, *req.DayFrom)
		where = append(where, fmt.Sprintf("day_timestamp >= $%d", len(args)))
	}

	if req.DayTo != nil {
		args = append(args, *req.DayTo)
		where = append(where, fmt.Sprintf("day_timestamp <= $%d", len(args)))
	}

	if strings.TrimSpace(req.InstitutionID) != "" {
		id, err := normalizeBytes32(req.InstitutionID)
		if err != nil {
			return nil, dailyCommitmentsValidationError(
				"institution_id", "invalid institution_id: %v", err)
		}

		args = append(args, id)
		where = append(where, fmt.Sprintf("institution_id = $%d", len(args)))
	}

	if strings.TrimSpace(req.DataType) != "" {
		dataType, err := normalizeBytes32(req.DataType)
		if err != nil {
			return nil, dailyCommitmentsValidationError(
				"data_type", "invalid data_type: %v", err)
		}

		args = append(args, dataType)
		where = append(where, fmt.Sprintf("data_type = $%d", len(args)))
	}

	args = append(args, req.Limit, req.Offset)

	//nolint:gosec // SQL fragments are selected locally; all request values remain parameters.
	query := fmt.Sprintf(`
		SELECT factory_address, day_timestamp, data_type, institution_id,
			daily_contract_address, commitment_count, discovery_block
		FROM chain.daily_commitment_stats
		WHERE %s
		ORDER BY day_timestamp DESC, factory_address, institution_id, data_type
		LIMIT $%d OFFSET $%d
	`, strings.Join(where, " AND "), len(args)-1, len(args))

	conn := getDB()
	if conn == nil {
		return nil, errDBConnectionFailed
	}

	rows, err := conn.Query(query, args...)
	if err != nil {
		log.Printf("api_storage: daily commitment stats query: %v", err)

		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	items := make([]DailyCommitmentItem, 0, req.Limit)

	for rows.Next() {
		var item DailyCommitmentItem

		if err := rows.Scan(
			&item.FactoryAddress,
			&item.DayTimestamp,
			&item.DataType,
			&item.InstitutionID,
			&item.DailyContractAddress,
			&item.CommitmentCount,
			&item.DiscoveryBlock,
		); err != nil {
			return nil, fmt.Errorf("scan daily commitment stats: %w", err)
		}

		item.FactoryAddress = common.HexToAddress(item.FactoryAddress).Hex()
		item.DailyContractAddress = common.HexToAddress(item.DailyContractAddress).Hex()
		item.DataType = common.HexToHash(item.DataType).Hex()
		item.InstitutionID = common.HexToHash(item.InstitutionID).Hex()
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate daily commitment stats: %w", err)
	}

	return &DailyCommitmentsResponse{
		List:   items,
		Limit:  req.Limit,
		Offset: req.Offset,
	}, nil
}

func normalizeBytes32(value string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) != 66 || !strings.HasPrefix(value, "0x") {
		return "", fmt.Errorf("must be a 0x-prefixed 32-byte hexadecimal value")
	}

	for _, c := range value[2:] {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return "", fmt.Errorf("must be a 0x-prefixed 32-byte hexadecimal value")
		}
	}

	return common.HexToHash(value).Hex(), nil
}
