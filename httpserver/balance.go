package httpserver

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

const balanceRPCTimeout = 5 * time.Second

// BalanceReader obtains a native account balance from a chain node.
type BalanceReader interface {
	BalanceAt(ctx context.Context, address common.Address, block string) (*big.Int, error)
}

type nodeBalanceReader struct {
	client *metrics.InstrumentedRPCClient
}

// NewNodeBalanceReader dials nodeRPC and returns a BalanceReader that calls eth_getBalance.
func NewNodeBalanceReader(nodeRPC string) (BalanceReader, error) {
	nodeRPC = strings.TrimSpace(nodeRPC)
	if nodeRPC == "" {
		return nil, fmt.Errorf("node RPC URL is empty")
	}

	client, err := rpc.Dial(nodeRPC)
	if err != nil {
		return nil, fmt.Errorf("dial node RPC: %w", err)
	}

	return &nodeBalanceReader{client: metrics.NewInstrumentedRPCClient(client)}, nil
}

func (r *nodeBalanceReader) BalanceAt(
	ctx context.Context,
	address common.Address,
	block string,
) (*big.Int, error) {
	blockArg, err := toBalanceBlockArg(block)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, balanceRPCTimeout)
	defer cancel()

	var result hexutil.Big
	if err := r.client.CallContext(ctx, &result, "eth_getBalance", address, blockArg); err != nil {
		return nil, err
	}

	return (*big.Int)(&result), nil
}

// toBalanceBlockArg maps "latest" or a decimal block number to an eth_getBalance block argument.
func toBalanceBlockArg(block string) (any, error) {
	block = strings.TrimSpace(block)
	if block == "" || strings.EqualFold(block, "latest") {
		return "latest", nil
	}

	if strings.EqualFold(block, "pending") || strings.EqualFold(block, "earliest") {
		return nil, fmt.Errorf("unsupported block tag %q", block)
	}

	n, err := strconv.ParseUint(block, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid block number %q", block)
	}

	return hexutil.EncodeUint64(n), nil
}
