package helper

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
)

var (
	DailyDeployedTopic = crypto.Keccak256Hash(
		[]byte("DailyDeployed(uint256,bytes32,bytes32,address,bytes32)"))
	CommittedTopic = crypto.Keccak256Hash([]byte("Committed(bytes32)"))
)

type DailyDeployedEvent struct {
	DayTimestamp         uint64
	InstitutionID        common.Hash
	DataType             common.Hash
	DailyContractAddress common.Address
	Salt                 common.Hash
}

func DecodeDailyDeployedLog(log *types.ReceiptLog, factory common.Address) (DailyDeployedEvent, bool) {
	event, reason := decodeDailyDeployedLog(log, factory)

	return event, reason == ""
}

// DailyDeployedRejectReason explains why DecodeDailyDeployedLog would reject a log.
// Empty string means the log is valid.
func DailyDeployedRejectReason(log *types.ReceiptLog, factory common.Address) string {
	_, reason := decodeDailyDeployedLog(log, factory)

	return reason
}

func decodeDailyDeployedLog(
	log *types.ReceiptLog,
	factory common.Address,
) (DailyDeployedEvent, string) {
	if log == nil {
		return DailyDeployedEvent{}, "nil log"
	}

	if factory == ZeroAddr {
		return DailyDeployedEvent{}, "zero factory address"
	}

	if !common.IsHexAddress(log.Address) {
		return DailyDeployedEvent{}, "invalid emitter address"
	}

	if common.HexToAddress(log.Address) != factory {
		return DailyDeployedEvent{}, "emitter is not the watched factory"
	}

	if len(log.Topics) != 4 {
		return DailyDeployedEvent{}, fmt.Sprintf("want 4 topics got %d", len(log.Topics))
	}

	if !validHashString(log.Topics[0]) || common.HexToHash(log.Topics[0]) != DailyDeployedTopic {
		return DailyDeployedEvent{}, "topic0 is not DailyDeployed"
	}

	if !validHashString(log.Topics[1]) || !validHashString(log.Topics[2]) ||
		!validHashString(log.Topics[3]) {
		return DailyDeployedEvent{}, "indexed topics are not 32-byte hashes"
	}

	dayValue := new(big.Int).SetBytes(common.HexToHash(log.Topics[1]).Bytes())
	if !dayValue.IsInt64() {
		return DailyDeployedEvent{}, "day timestamp does not fit int64"
	}

	day := dayValue.Uint64()
	institutionID := common.HexToHash(log.Topics[2])
	dataType := common.HexToHash(log.Topics[3])

	if day == 0 || day%86400 != 0 {
		return DailyDeployedEvent{}, fmt.Sprintf(
			"day timestamp %d is not a non-zero UTC day boundary", day)
	}

	if institutionID == (common.Hash{}) {
		return DailyDeployedEvent{}, "zero institution id"
	}

	if dataType == (common.Hash{}) {
		return DailyDeployedEvent{}, "zero data type"
	}

	data, err := hexutil.Decode(log.Data)
	if err != nil {
		return DailyDeployedEvent{}, "invalid log data hex"
	}

	if len(data) != 64 {
		return DailyDeployedEvent{}, fmt.Sprintf("want 64 data bytes got %d", len(data))
	}

	for _, prefixByte := range data[:12] {
		if prefixByte != 0 {
			return DailyDeployedEvent{}, "daily address word has non-zero prefix"
		}
	}

	dailyAddress := common.BytesToAddress(data[12:32])
	salt := common.BytesToHash(data[32:64])

	expectedSalt := crypto.Keccak256Hash(
		common.BigToHash(dayValue).Bytes(),
		institutionID.Bytes(),
		dataType.Bytes(),
	)

	if dailyAddress == ZeroAddr {
		return DailyDeployedEvent{}, "zero daily contract address"
	}

	if salt != expectedSalt {
		return DailyDeployedEvent{}, "salt does not match computeSalt(day, institution, dataType)"
	}

	return DailyDeployedEvent{
		DayTimestamp:         day,
		InstitutionID:        institutionID,
		DataType:             dataType,
		DailyContractAddress: dailyAddress,
		Salt:                 salt,
	}, ""
}

func DecodeCommittedLog(log *types.ReceiptLog, dailyContract common.Address) (common.Hash, bool) {
	hash, reason := decodeCommittedLog(log, dailyContract)

	return hash, reason == ""
}

// CommittedRejectReason explains why DecodeCommittedLog would reject a log.
// Empty string means the log is valid.
func CommittedRejectReason(log *types.ReceiptLog, dailyContract common.Address) string {
	_, reason := decodeCommittedLog(log, dailyContract)

	return reason
}

func decodeCommittedLog(
	log *types.ReceiptLog,
	dailyContract common.Address,
) (common.Hash, string) {
	if log == nil {
		return common.Hash{}, "nil log"
	}

	if dailyContract == ZeroAddr {
		return common.Hash{}, "zero daily contract address"
	}

	if !common.IsHexAddress(log.Address) {
		return common.Hash{}, "invalid emitter address"
	}

	if common.HexToAddress(log.Address) != dailyContract {
		return common.Hash{}, "emitter is not the expected daily contract"
	}

	if len(log.Topics) != 2 {
		return common.Hash{}, fmt.Sprintf("want 2 topics got %d", len(log.Topics))
	}

	if !validHashString(log.Topics[0]) || common.HexToHash(log.Topics[0]) != CommittedTopic {
		return common.Hash{}, "topic0 is not Committed"
	}

	if !validHashString(log.Topics[1]) {
		return common.Hash{}, "commitment hash topic is not a 32-byte hash"
	}

	data, err := hexutil.Decode(log.Data)
	if err != nil {
		return common.Hash{}, "invalid log data hex"
	}

	if len(data) != 0 {
		return common.Hash{}, fmt.Sprintf("want empty data got %d bytes", len(data))
	}

	return common.HexToHash(log.Topics[1]), ""
}

func validHashString(value string) bool {
	if len(value) != 66 || !strings.HasPrefix(value, "0x") {
		return false
	}

	decoded, err := hexutil.Decode(value)

	return err == nil && len(decoded) == common.HashLength
}
