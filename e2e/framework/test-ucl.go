package framework

import (
	"context"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

//go:embed erc20.bytecode
var Erc20Bytecode string

// Compiled from ucl-oracle/contracts at Solidity 0.8.24 with optimizer_runs=200.
//
//go:embed oracle_daily_commitment_factory.bytecode
var dailyCommitmentFactoryBytecode string

//go:embed oracle_mock_institution_registry.bytecode
var mockInstitutionRegistryBytecode string

var Erc20ConstructorMintAmount, _ = new(big.Int).SetString("1000000000000000000000", 10)

const mockInstitutionRegistryABI = `[
	{"type":"function","name":"setExists","stateMutability":"nonpayable",
	 "inputs":[{"name":"institutionId","type":"bytes32"},{"name":"value","type":"bool"}],"outputs":[]},
	{"type":"function","name":"setPublisher","stateMutability":"nonpayable",
	 "inputs":[{"name":"institutionId","type":"bytes32"},{"name":"publisher","type":"address"},
	 {"name":"authorized","type":"bool"}],"outputs":[]}
]`

const dailyCommitmentFactoryABI = `[
	{"type":"constructor","stateMutability":"nonpayable",
	 "inputs":[{"name":"registryAddress","type":"address"}]},
	{"type":"function","name":"getOrDeployDaily","stateMutability":"nonpayable",
	 "inputs":[{"name":"dayTs","type":"uint256"},{"name":"institutionId","type":"bytes32"},
	 {"name":"dataType","type":"bytes32"}],"outputs":[{"name":"","type":"address"}]},
	{"type":"event","name":"DailyDeployed","anonymous":false,
	 "inputs":[{"name":"dayTs","type":"uint256","indexed":true},
	 {"name":"institutionId","type":"bytes32","indexed":true},
	 {"name":"dataType","type":"bytes32","indexed":true},
	 {"name":"dailyContract","type":"address","indexed":false},
	 {"name":"salt","type":"bytes32","indexed":false}]}
]`

const dailyCommitmentABI = `[
	{"type":"function","name":"commit","stateMutability":"nonpayable",
	 "inputs":[{"name":"hashes","type":"bytes32[]"}],"outputs":[]},
	{"type":"function","name":"commitmentCount","stateMutability":"view",
	 "inputs":[],"outputs":[{"name":"","type":"uint256"}]}
]`

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
	u.WaitForBlock(1, 5*time.Minute)

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

func (u *UCL) sendTx(
	privateKey string,
	to *common.Address,
	data []byte,
	value *big.Int,
	gasLimit uint64) *types.Receipt {
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
	for i := 0; i < 60; i++ {
		receipt, err = u.client.TransactionReceipt(ctx, signedTx.Hash())
		if err == nil {
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	if receipt == nil {
		u.t.Fatalf("failed to get receipt after 60 attempts")
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

func (u *UCL) DeployMockInstitutionRegistry(privateKey string) *types.Receipt {
	return u.deployContract(privateKey, mockInstitutionRegistryBytecode, nil, 3000000)
}

func (u *UCL) ConfigureMockInstitution(
	privateKey string,
	registry common.Address,
	institutionID common.Hash,
	publisher common.Address,
) {
	registryABI := u.parseABI(mockInstitutionRegistryABI)

	setExists, err := registryABI.Pack("setExists", institutionID, true)
	if err != nil {
		u.t.Fatalf("encode mock registry setExists call: %v", err)
	}

	u.sendTx(privateKey, &registry, setExists, big.NewInt(0), 200000)

	setPublisher, err := registryABI.Pack(
		"setPublisher",
		institutionID,
		publisher,
		true,
	)
	if err != nil {
		u.t.Fatalf("encode mock registry setPublisher call: %v", err)
	}

	u.sendTx(privateKey, &registry, setPublisher, big.NewInt(0), 300000)
}

func (u *UCL) DeployDailyCommitmentFactory(
	privateKey string,
	registry common.Address,
) *types.Receipt {
	factoryABI := u.parseABI(dailyCommitmentFactoryABI)

	constructor, err := factoryABI.Pack("", registry)
	if err != nil {
		u.t.Fatalf("encode DailyCommitmentFactory constructor: %v", err)
	}

	return u.deployContract(
		privateKey,
		dailyCommitmentFactoryBytecode,
		constructor,
		10000000,
	)
}

func (u *UCL) DeployDailyCommitment(
	privateKey string,
	factory common.Address,
	dayTimestamp uint64,
	institutionID common.Hash,
	dataType common.Hash,
) (*types.Receipt, common.Address) {
	factoryABI := u.parseABI(dailyCommitmentFactoryABI)

	call, err := factoryABI.Pack(
		"getOrDeployDaily",
		new(big.Int).SetUint64(dayTimestamp),
		institutionID,
		dataType,
	)
	if err != nil {
		u.t.Fatalf("encode getOrDeployDaily call: %v", err)
	}

	receipt := u.sendTx(privateKey, &factory, call, big.NewInt(0), 10000000)
	event := factoryABI.Events["DailyDeployed"]

	for _, entry := range receipt.Logs {
		if entry.Address != factory || len(entry.Topics) != 4 || entry.Topics[0] != event.ID {
			continue
		}

		values, err := event.Inputs.NonIndexed().Unpack(entry.Data)
		if err != nil {
			u.t.Fatalf("decode DailyDeployed event: %v", err)
		}

		if len(values) != 2 {
			u.t.Fatalf("DailyDeployed data field count: got %d want 2", len(values))
		}

		dailyAddress, ok := values[0].(common.Address)
		if !ok || dailyAddress == (common.Address{}) {
			u.t.Fatalf("DailyDeployed daily contract has unexpected type/value %T %v",
				values[0], values[0])
		}

		return receipt, dailyAddress
	}

	u.t.Fatal("getOrDeployDaily receipt did not contain DailyDeployed")

	return nil, common.Address{}
}

func (u *UCL) CommitDaily(
	privateKey string,
	daily common.Address,
	hashes []common.Hash,
) *types.Receipt {
	dailyABI := u.parseABI(dailyCommitmentABI)

	call, err := dailyABI.Pack("commit", hashes)
	if err != nil {
		u.t.Fatalf("encode DailyCommitment commit call: %v", err)
	}

	return u.sendTx(privateKey, &daily, call, big.NewInt(0), 3000000)
}

func (u *UCL) DailyCommitmentCount(daily common.Address) uint64 {
	dailyABI := u.parseABI(dailyCommitmentABI)

	call, err := dailyABI.Pack("commitmentCount")
	if err != nil {
		u.t.Fatalf("encode DailyCommitment commitmentCount call: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := u.client.CallContract(ctx, ethereum.CallMsg{To: &daily, Data: call}, nil)
	if err != nil {
		u.t.Fatalf("call DailyCommitment commitmentCount: %v", err)
	}

	values, err := dailyABI.Unpack("commitmentCount", result)
	if err != nil {
		u.t.Fatalf("decode DailyCommitment commitmentCount: %v", err)
	}

	if len(values) != 1 {
		u.t.Fatalf("commitmentCount result count: got %d want 1", len(values))
	}

	count, ok := values[0].(*big.Int)
	if !ok || !count.IsUint64() {
		u.t.Fatalf("commitmentCount has unexpected type/value %T %v", values[0], values[0])
	}

	return count.Uint64()
}

func (u *UCL) deployContract(
	privateKey string,
	bytecode string,
	constructor []byte,
	gasLimit uint64,
) *types.Receipt {
	data, err := hex.DecodeString(strings.TrimPrefix(strings.TrimSpace(bytecode), "0x"))
	if err != nil {
		u.t.Fatalf("decode contract bytecode: %v", err)
	}

	data = append(data, constructor...)

	return u.sendTx(privateKey, nil, data, big.NewInt(0), gasLimit)
}

func (u *UCL) parseABI(definition string) abi.ABI {
	contractABI, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		u.t.Fatalf("parse contract ABI: %v", err)
	}

	return contractABI
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

// RestartNode stops the node at the given index, waits for `downtime`, then starts
// it again with an identical command line - same RPC port, same data dir.
// Used to simulate a node outage underneath the syncer, without restarting the syncer.
func (u *UCL) RestartNode(index int, downtime time.Duration) {
	if index >= len(nodesRpcPorts) {
		u.t.Fatalf("node index %d out of range (max %d)", index, len(nodesRpcPorts)-1)
	}

	port := nodesRpcPorts[index]
	pattern := fmt.Sprintf("jsonrpc :%d", port)

	// Find the node's PID by matching its RPC port in the command line. The node is
	// spawned by the UCL bash script, so we have no *exec.Cmd handle for it.
	out, err := exec.Command("pgrep", "-f", pattern).Output() //nolint:gosec
	if err != nil {
		u.t.Fatalf("node %d not running (pgrep %q): %v", index, pattern, err)
	}

	pid, err := strconv.Atoi(strings.Fields(string(out))[0])
	if err != nil {
		u.t.Fatalf("failed to parse pid for node %d: %v", index, err)
	}

	// Capture the exact command line and working directory BEFORE killing the process -
	// this is what lets us bring the node back up on the same port with the same flags.
	// Linux-only (/proc).
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		u.t.Fatalf("failed to read cmdline of node %d: %v", index, err)
	}

	// /proc/<pid>/cmdline is NUL-separated with a trailing NUL.
	args := strings.Split(strings.TrimRight(string(raw), "\x00"), "\x00")
	if len(args) == 0 {
		u.t.Fatalf("empty cmdline for node %d", index)
	}

	cwd, err := os.Readlink(fmt.Sprintf("/proc/%d/cwd", pid))
	if err != nil {
		u.t.Logf("failed to read cwd of node %d, using test cwd: %v", index, err)

		cwd = ""
	}

	u.t.Logf("stopping node %d (pid %d, port %d) for %s", index, pid, port, downtime)

	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		u.t.Fatalf("failed to SIGTERM node %d: %v", index, err)
	}

	// Wait for the process to actually exit; fall back to SIGKILL if it hangs.
	// Signal 0 only checks whether the process still exists.
	deadline := time.Now().UTC().Add(10 * time.Second)

	for {
		if err := syscall.Kill(pid, 0); err != nil { // process is gone
			break
		}

		if time.Now().UTC().After(deadline) {
			u.t.Logf("node %d did not exit on SIGTERM, sending SIGKILL", index)

			_ = syscall.Kill(pid, syscall.SIGKILL)

			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// The port must be released before we re-exec, otherwise the restart fails with
	// "bind: address already in use".
	waitPortClosed(u.t, port, 10*time.Second)

	// The outage window. The syncer should be retrying against a dead RPC here.
	time.Sleep(downtime)

	// Append to the same log file Start() uses, so the restarted node does not spam
	// the test console. O_APPEND (not O_TRUNC) keeps everything logged before the outage.
	f, err := os.OpenFile(
		filepath.Join(u.logsDir, "ucl.log"),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		u.t.Fatalf("failed to open ucl log file for restarted node %d: %v", index, err)
	}

	// Re-exec with the exact same argv and cwd -> same RPC port, same data dir.
	// The node resyncs from its peers and picks up where it left off.
	cmd := exec.Command(args[0], args[1:]...) //nolint:gosec
	cmd.Dir = cwd
	cmd.Stdout = f
	cmd.Stderr = f

	if err := cmd.Start(); err != nil {
		f.Close() //nolint:errcheck

		u.t.Fatalf("failed to restart node %d: %v", index, err)
	}

	// The restarted node is no longer a child of the UCL script, so Stop() will not
	// reap it - clean it up here.
	u.t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = f.Close()
	})

	waitPortOpen(u.t, port, 30*time.Second)

	u.t.Logf("node %d back up on port %d (pid %d)", index, port, cmd.Process.Pid)
}

// WaitForNonce blocks until addr's mined nonce (as seen by the given client)
// reaches want, or the timeout expires. Takes an explicit client on purpose:
// pass a live peer when the node behind u.Client() is the one under test.
func (u *UCL) WaitForNonce(addr common.Address, want uint64, timeout time.Duration) {
	deadline := time.Now().UTC().Add(timeout)
	for time.Now().UTC().Before(deadline) {
		n, err := u.client.NonceAt(context.Background(), addr, nil) // nil = latest mined
		if err != nil {
			u.t.Fatalf("failed to read nonce for %s: %v", addr.Hex(), err)
		}

		if n >= want {
			return
		}

		time.Sleep(time.Second)
	}

	u.t.Fatalf("timeout: %s mined nonce did not reach %d within %s", addr.Hex(), want, timeout)
}

// waitPortClosed blocks until nothing accepts connections on the port, or the timeout expires.
func waitPortClosed(t *testing.T, port int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().UTC().Add(timeout)
	for time.Now().UTC().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err != nil {
			return
		}

		_ = conn.Close()

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("port %d still open after %s", port, timeout)
}

// waitPortOpen blocks until the port accepts connections, or the timeout expires.
// A successful dial only means the listener is up - the RPC may still be warming up.
func waitPortOpen(t *testing.T, port int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().UTC().Add(timeout)
	for time.Now().UTC().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()

			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("port %d did not open within %s", port, timeout)
}
