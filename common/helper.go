package common

import (
	"errors"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

func NormalizeAddress(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", errors.New("empty addr string")
	}

	if common.IsHexAddress(addr) {
		return common.HexToAddress(addr).Hex(), nil
	}

	return "", errors.New("invalid address")
}
