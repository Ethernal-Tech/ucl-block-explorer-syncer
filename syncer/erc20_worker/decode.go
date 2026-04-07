package erc20_worker

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// TransferTopic is the keccak256 hash of Transfer(address,address,uint256).
var TransferTopic = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

var zeroAddr = common.Address{}

// OmitTransferFromStats is true for decoded Transfer logs we do not aggregate. Only
// Transfer(0,0,value) is omitted — it is not a meaningful mint, burn, or peer transfer.
// Zero-value Transfer events are still counted (they are valid logs).
func OmitTransferFromStats(from, to common.Address) bool {
	return from == zeroAddr && to == zeroAddr
}

// ClassifyTransfer labels a standard ERC-20 Transfer using zero-address conventions.
// Caller must skip OmitTransferFromStats first (so from/to are not both zero).
func ClassifyTransfer(from, to common.Address) string {
	switch {
	case from == zeroAddr:
		return "mint"
	case to == zeroAddr:
		return "burn"
	default:
		return "transfer"
	}
}

// DecodeTransferLog returns token (log contract), from, to, value if log is a valid Transfer.
func DecodeTransferLog(logAddr string, topics []string, data string) (token, from, to common.Address, value *big.Int, ok bool) {
	if len(topics) < 3 {
		return common.Address{}, common.Address{}, common.Address{}, nil, false
	}
	topic0 := common.HexToHash(topics[0])
	if topic0 != TransferTopic {
		return common.Address{}, common.Address{}, common.Address{}, nil, false
	}
	if !common.IsHexAddress(logAddr) {
		return common.Address{}, common.Address{}, common.Address{}, nil, false
	}
	token = common.HexToAddress(logAddr)
	from = common.BytesToAddress(common.HexToHash(topics[1]).Bytes())
	to = common.BytesToAddress(common.HexToHash(topics[2]).Bytes())
	v, err := parseUint256Data(data)
	if err != nil {
		return common.Address{}, common.Address{}, common.Address{}, nil, false
	}
	return token, from, to, v, true
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
