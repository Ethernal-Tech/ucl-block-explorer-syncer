package framework

import (
	"context"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

//go:embed erc20.bytecode
var Erc20Bytecode string

var nodesRpcPorts = []int{10002, 20002, 30002, 40002}

type UCL struct {
	node    *node
	config  UCLConfig
	client  *ethclient.Client
	logsDir string
	t       *testing.T
}

func NewUCL(t *testing.T, cfg UCLConfig, logsDir string) *UCL {
	t.Helper()

	return &UCL{t: t, config: cfg, logsDir: logsDir}
}

func (u *UCL) Start() {
	u.t.Cleanup(func() {
		u.Stop()
	})

	f, err := os.OpenFile(filepath.Join(u.logsDir, "ucl.log"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		u.t.Fatalf("failed to create ucl log file: %v", err)
	}

	args := append([]string{u.config.UclScript, "ibft"}, u.config.Flags...)

	u.config.RpcUrl = u.NodeRpcUrl(0)

	n, err := newNode("bash", args, f, u.config.Dir)
	if err != nil {
		u.t.Fatalf("failed to start ucl: %v", err)
	}

	u.node = n
	u.WaitForBlock(1, time.Minute)

	client, err := ethclient.Dial(u.config.RpcUrl)
	if err != nil {
		u.t.Fatalf("failed to connect to eth client: %v", err)
	}

	u.client = client
}

func (u *UCL) Stop() {
	if u.client != nil {
		u.client.Close()
		u.client = nil
	}

	if u.node == nil || u.node.cmd == nil {
		return
	}

	syscall.Kill(-u.node.cmd.Process.Pid, syscall.SIGTERM) //nolint:errcheck

	select {
	case <-u.node.Wait():
	case <-time.After(10 * time.Second):
		syscall.Kill(-u.node.cmd.Process.Pid, syscall.SIGKILL) //nolint:errcheck
	}

	// kill any leftover node processes
	for _, port := range nodesRpcPorts {
		exec.Command("pkill", "-f", fmt.Sprintf("jsonrpc :%d", port)).Run() //nolint:gosec,errcheck
	}
}

func (u *UCL) Client() *ethclient.Client {
	return u.client
}

func (u *UCL) IsRunning() bool {
	return u.node != nil && u.node.cmd != nil
}

func (u *UCL) StopNode(index int) {
	if index >= len(nodesRpcPorts) {
		u.t.Fatalf("node index %d out of range (max %d)", index, len(nodesRpcPorts)-1)
	}

	cmd := exec.Command("pkill", "-f", fmt.Sprintf("jsonrpc :%d", nodesRpcPorts[index])) //nolint:gosec
	if err := cmd.Run(); err != nil {
		u.t.Logf("failed to stop node %d: %v", index, err)
	}

	u.t.Logf("stopped node %d (port %d)", index, nodesRpcPorts[index])
}

func (u *UCL) NodeRpcUrl(index int) string {
	if index >= len(nodesRpcPorts) {
		u.t.Fatalf("node index %d out of range (max %d)", index, len(nodesRpcPorts)-1)
	}

	return fmt.Sprintf("http://localhost:%d", nodesRpcPorts[index])
}

func (u *UCL) ChangeNodeRpcUrl(index int) {
	url := u.NodeRpcUrl(index)
	u.config.RpcUrl = url

	if u.client != nil {
		u.client.Close()
	}

	client, err := ethclient.Dial(url)
	if err != nil {
		u.t.Fatalf("failed to reconnect eth client: %v", err)
	}

	u.client = client
}

func (u *UCL) WaitForBlock(target uint64, timeout time.Duration) {
	deadline := time.Now().UTC().Add(timeout)
	for time.Now().UTC().Before(deadline) {
		num, err := u.getBlockNumber()
		if err == nil && num >= target {
			u.t.Logf("ucl ready, block %d", num)

			return
		}

		time.Sleep(2 * time.Second)
	}

	u.t.Fatalf("ucl not ready after %s", timeout)
}

func (u *UCL) getBlockNumber() (uint64, error) {
	resp, err := http.Post(
		u.config.RpcUrl,
		"application/json",
		strings.NewReader(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`),
	)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close() //nolint:errcheck

	var result struct {
		Result string `json:"result"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	num, err := strconv.ParseUint(strings.TrimPrefix(result.Result, "0x"), 16, 64)
	if err != nil {
		return 0, err
	}

	return num, nil
}

func (u *UCL) sendTx(privateKey string, to *common.Address, data []byte, value *big.Int, gasLimit uint64) *types.Receipt {
	ctx := context.Background()

	pk, err := crypto.HexToECDSA(strings.TrimPrefix(privateKey, "0x"))
	if err != nil {
		u.t.Fatalf("failed to parse private key: %s", err)
	}

	addr := crypto.PubkeyToAddress(pk.PublicKey)

	chainID, err := u.client.ChainID(ctx)
	if err != nil {
		u.t.Fatalf("failed to get chain ID: %s", err)
	}

	nonce, err := u.client.PendingNonceAt(ctx, addr)
	if err != nil {
		u.t.Fatalf("failed to get nonce: %s", err)
	}

	gasPrice, err := u.client.SuggestGasPrice(ctx)
	if err != nil {
		u.t.Fatalf("failed to get gas price: %s", err)
	}

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       to,
		Value:    value,
		Gas:      gasLimit,
		GasPrice: gasPrice,
		Data:     data,
	})

	signedTx, err := types.SignTx(tx, types.NewLondonSigner(chainID), pk)
	if err != nil {
		u.t.Fatalf("failed to sign tx: %s", err)
	}

	if err := u.client.SendTransaction(ctx, signedTx); err != nil {
		u.t.Fatalf("failed to send tx: %s", err)
	}

	var receipt *types.Receipt
	for i := 0; i < 30; i++ {
		receipt, err = u.client.TransactionReceipt(ctx, signedTx.Hash())
		if err == nil {
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	if receipt == nil {
		u.t.Fatalf("failed to get receipt after 30 attempts")
	}

	if receipt.Status != types.ReceiptStatusSuccessful {
		u.t.Fatalf("tx failed, status: %d", receipt.Status)
	}

	return receipt
}

func (u *UCL) SendNativeTokens(privateKey string, to common.Address, amount *big.Int) *types.Receipt {
	return u.sendTx(privateKey, &to, nil, amount, 21000)
}

func (u *UCL) DeployERC20(privateKey string) *types.Receipt {
	data, err := hex.DecodeString(strings.TrimPrefix(Erc20Bytecode, "0x"))
	if err != nil {
		u.t.Fatalf("failed to decode bytecode: %s", err)
	}

	return u.sendTx(privateKey, nil, data, big.NewInt(0), 3000000)
}

func (u *UCL) MintERC20(privateKey string, contractAddr, to common.Address, amount *big.Int) *types.Receipt {
	selector := crypto.Keccak256([]byte("mint(address,uint256)"))[:4]
	data := make([]byte, 0, 68)
	data = append(data, selector...)
	data = append(data, common.LeftPadBytes(to.Bytes(), 32)...)
	data = append(data, common.LeftPadBytes(amount.Bytes(), 32)...)

	return u.sendTx(privateKey, &contractAddr, data, big.NewInt(0), 200000)
}

func (u *UCL) BurnERC20(privateKey string, contractAddr common.Address, amount *big.Int) *types.Receipt {
	selector := crypto.Keccak256([]byte("burn(uint256)"))[:4]
	data := make([]byte, 0, 36)
	data = append(data, selector...)
	data = append(data, common.LeftPadBytes(amount.Bytes(), 32)...)

	return u.sendTx(privateKey, &contractAddr, data, big.NewInt(0), 200000)
}

func (u *UCL) TransferERC20(privateKey string, contractAddr, to common.Address, amount *big.Int) *types.Receipt {
	selector := crypto.Keccak256([]byte("transfer(address,uint256)"))[:4]
	data := make([]byte, 0, 68)
	data = append(data, selector...)
	data = append(data, common.LeftPadBytes(to.Bytes(), 32)...)
	data = append(data, common.LeftPadBytes(amount.Bytes(), 32)...)

	return u.sendTx(privateKey, &contractAddr, data, big.NewInt(0), 200000)
}
