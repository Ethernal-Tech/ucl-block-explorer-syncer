package helper

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	txworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/tx_worker"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func CreateJobs(txCount, workerCount uint64) []txworker.TxJob {
	if txCount == 0 {
		return nil
	}

	activeWorkers := min(txCount, workerCount)

	base := txCount / activeWorkers
	extra := txCount % activeWorkers

	ranges := make([]txworker.TxJob, 0, activeWorkers)
	cursor := uint64(0)

	for i := range activeWorkers {
		size := base
		if i < extra {
			size++
		}

		ranges = append(ranges, txworker.TxJob{
			From: uint64(cursor),
			To:   uint64(cursor) + size,
		})

		cursor += size
	}

	return ranges
}

// TransferTopic is the keccak256 hash of ERC-20 Transfer event.
var TransferTopic = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

var ZeroAddr = common.Address{}

// ClassifyTransfer classifies an ERC-20 Transfer event as a mint, burn, or transfer based
// on zero-address conventions: a mint has the zero address as the sender, a burn has the
// zero address as the recipient, and everything else is a regular transfer.
func ClassifyTransfer(from, to common.Address) string {
	switch {
	case from == ZeroAddr:
		return "mint"
	case to == ZeroAddr:
		return "burn"
	default:
		return "transfer"
	}
}

// DecodeTransferLog checks whether the provided log (topics and data) is the result of an
// emitted ERC-20 Transfer event. If not, `ok` is false. Otherwise, the function returns the
// `from` and `to` addresses along with the transferred value.
func DecodeTransferLog(
	topics []string,
	data string) (from, to common.Address, value *big.Int, ok bool) {
	if len(topics) < 3 {
		return ZeroAddr, ZeroAddr, nil, false
	}

	if common.HexToHash(topics[0]) != TransferTopic {
		return ZeroAddr, ZeroAddr, nil, false
	}

	from = common.BytesToAddress(common.HexToHash(topics[1]).Bytes())
	to = common.BytesToAddress(common.HexToHash(topics[2]).Bytes())

	v, err := parseUint256Data(data)
	if err != nil {
		return ZeroAddr, ZeroAddr, nil, false
	}

	return from, to, v, true
}

func parseUint256Data(data string) (*big.Int, error) {
	s := strings.TrimSpace(data)
	if s == "" || s == "0x" {
		return big.NewInt(0), nil
	}
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return nil, err
	}
	if len(b) > 32 {
		return nil, fmt.Errorf("data too long")
	}
	return new(big.Int).SetBytes(b), nil
}

// DefaultLogger logs syncer state changes and actions to standard output using fmt formatting.
type DefaultLogger struct{}

// Log logs to standard output using fmt formatting.
func (DefaultLogger) Log(log string) {
	fmt.Println(log)
}
