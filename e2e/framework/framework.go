package framework

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

type TestSyncerConfig struct {
	t *testing.T

	// Edge
	EdgeFlags  []string
	EdgeScript string

	// DB
	ComposeDir string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string

	// Syncer
	RpcUrl            string
	ConnString        string
	Logging           bool
	PollInterval      uint64
	TipOnly           bool
	FullBlock         bool
	BatchSize         uint64
	TxWorkers         uint64
	Erc20Stats        bool
	Erc20StartFromTip bool
	EoaActivityStats  bool

	// Logging
	LogsDir string
}

func (c *TestSyncerConfig) GetConnString() string {
	if c.ConnString != "" {
		return c.ConnString
	}

	return fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable",
		c.DBUser, c.DBPassword, c.DBPort, c.DBName)
}

func (c *TestSyncerConfig) SyncerArgs() []string {
	args := []string{
		"run", ".", "sync",
		"--rpc-url", c.RpcUrl,
		"--db-conn", c.GetConnString(),
		"--poll-interval", strconv.FormatUint(c.PollInterval, 10),
		"--batch-size", strconv.FormatUint(c.BatchSize, 10),
		"--tx-workers", strconv.FormatUint(c.TxWorkers, 10),
	}

	if c.Logging {
		args = append(args, "--logging")
	}

	if c.TipOnly {
		args = append(args, "--tip-only")
	}

	if c.FullBlock {
		args = append(args, "--full-block")
	}

	if c.Erc20Stats {
		args = append(args, "--erc20-stats")
		if c.Erc20StartFromTip {
			args = append(args, "--erc20-start-from-tip")
		}
	}

	if c.EoaActivityStats {
		args = append(args, "--eoa-activity-stats")
	}

	return args
}

func defaultConfig(t *testing.T) *TestSyncerConfig {
	t.Helper()

	return &TestSyncerConfig{
		t:            t,
		EdgeFlags:    []string{"write-logs"},
		EdgeScript:   "../ucl/scripts/cluster_syncer.sh",
		ComposeDir:   "../docker/db",
		DBPort:       "5433",
		DBUser:       "syncer",
		DBPassword:   "syncer",
		DBName:       "syncer_e2e",
		RpcUrl:       "http://localhost:10002",
		PollInterval: 2000,
		BatchSize:    1,
		TxWorkers:    1,
		LogsDir:      "./logs",
	}
}

// --- Options ---

type SyncerOption func(*TestSyncerConfig)

func WithEdgeFlags(flags ...string) SyncerOption {
	return func(cfg *TestSyncerConfig) {
		cfg.EdgeFlags = flags
	}
}

func WithDBPort(port string) SyncerOption {
	return func(cfg *TestSyncerConfig) {
		cfg.DBPort = port
	}
}

func WithErc20Stats() SyncerOption {
	return func(cfg *TestSyncerConfig) {
		cfg.Erc20Stats = true
	}
}

func WithEoaActivity() SyncerOption {
	return func(cfg *TestSyncerConfig) {
		cfg.EoaActivityStats = true
	}
}

func WithFullBlock() SyncerOption {
	return func(cfg *TestSyncerConfig) {
		cfg.FullBlock = true
	}
}

func WithTipOnly() SyncerOption {
	return func(cfg *TestSyncerConfig) {
		cfg.TipOnly = true
	}
}

func WithLogging() SyncerOption {
	return func(cfg *TestSyncerConfig) {
		cfg.Logging = true
	}
}

func WithBatchSize(size uint64) SyncerOption {
	return func(cfg *TestSyncerConfig) {
		cfg.BatchSize = size
	}
}

func WithTxWorkers(workers uint64) SyncerOption {
	return func(cfg *TestSyncerConfig) {
		cfg.TxWorkers = workers
	}
}

// --- TestSyncer ---

type TestSyncer struct {
	Config    *TestSyncerConfig
	edgeCmd   *exec.Cmd
	syncerCmd *exec.Cmd
	dbStarted bool
	t         *testing.T
}

func NewTestSyncer(t *testing.T, opts ...SyncerOption) *TestSyncer {
	t.Helper()

	cfg := defaultConfig(t)

	for _, opt := range opts {
		opt(cfg)
	}

	ts := &TestSyncer{t: t, Config: cfg}
	ts.initLogsDir()

	// cleanup on ctrl+c
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		t.Log("caught interrupt, cleaning up...")
		ts.Stop()
		os.Exit(1)
	}()

	return ts
}
func (ts *TestSyncer) StartDB() {
	ts.t.Helper()

	// ensure logs dir exists
	if err := os.MkdirAll(ts.Config.LogsDir, 0750); err != nil {
		ts.t.Fatalf("failed to create logs dir: %v", err)
	}

	dockerComposeUp(ts.Config.ComposeDir, ts.Config.LogsDir)
	ts.dbStarted = true

	// wait for postgres readiness
	deadline := time.Now().UTC().Add(30 * time.Second)
	for time.Now().UTC().Before(deadline) {
		cmd := exec.Command("pg_isready", //nolint:gosec
			"-h", "localhost",
			"-p", ts.Config.DBPort,
			"-U", ts.Config.DBUser,
		)
		if cmd.Run() == nil {
			ts.t.Log("db ready")

			return
		}

		time.Sleep(time.Second)
	}

	ts.t.Fatal("db not ready after 30s")
}

func (ts *TestSyncer) StartUCL() {
	ts.t.Helper()

	f, err := os.OpenFile(filepath.Join(ts.Config.LogsDir, "ucl.log"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		ts.t.Fatalf("failed to create edge log file: %v", err)
	}

	args := append([]string{ts.Config.EdgeScript, "ibft"}, ts.Config.EdgeFlags...)
	ts.edgeCmd = exec.Command("bash", args...) //nolint:gosec
	ts.edgeCmd.Dir = "../ucl"
	ts.edgeCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	ts.edgeCmd.Stdout = f
	ts.edgeCmd.Stderr = f

	if err := ts.edgeCmd.Start(); err != nil {
		ts.t.Fatalf("failed to start edge: %v", err)
	}

	ts.WaitForBlock(1, time.Minute)
}

func (ts *TestSyncer) StartSyncer() {
	ts.t.Helper()

	f, err := os.OpenFile(filepath.Join(ts.Config.LogsDir, "syncer.log"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		ts.t.Fatalf("failed to create edge log file: %v", err)
	}

	ts.syncerCmd = exec.Command("go", ts.Config.SyncerArgs()...) //nolint:gosec
	ts.syncerCmd.Dir = ".."
	ts.syncerCmd.Stdout = f
	ts.syncerCmd.Stderr = f

	if err := ts.syncerCmd.Start(); err != nil {
		ts.t.Fatalf("failed to start syncer: %v", err)
	}
}

func (ts *TestSyncer) Stop() {
	if ts.syncerCmd != nil && ts.syncerCmd.Process != nil {
		ts.syncerCmd.Process.Kill() //nolint:errcheck
	}

	if ts.edgeCmd != nil && ts.edgeCmd.Process != nil {
		syscall.Kill(-ts.edgeCmd.Process.Pid, syscall.SIGKILL) //nolint:errcheck
	}

	if ts.dbStarted {
		dockerComposeDown(ts.Config.ComposeDir)
	}
}

func (ts *TestSyncer) WaitForBlock(target uint64, timeout time.Duration) {
	ts.t.Helper()

	deadline := time.Now().UTC().Add(timeout)

	for time.Now().UTC().Before(deadline) {
		num, err := ts.getBlockNumber()
		if err == nil && num >= target {
			ts.t.Logf("edge ready, block %d", num)

			return
		}

		time.Sleep(2 * time.Second)
	}

	ts.t.Fatalf("edge not ready after %s", timeout)
}

func (ts *TestSyncer) getBlockNumber() (uint64, error) {
	resp, err := http.Post(
		ts.Config.RpcUrl,
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

func (ts *TestSyncer) initLogsDir() {
	dir := filepath.Join("../e2e-logs", fmt.Sprintf("%s-%d", ts.t.Name(), time.Now().UTC().UnixMilli()))

	if err := os.MkdirAll(dir, 0750); err != nil {
		ts.t.Fatalf("failed to create logs dir: %v", err)
	}

	ts.Config.LogsDir = dir
	ts.t.Logf("logs dir: %s", dir)
}

// --- Docker helpers ---
func dockerComposeUp(composeDir, logsDir string) {
	f, err := os.OpenFile(filepath.Join(logsDir, "syncer-db.log"), os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		fmt.Printf("Error creating syncer-db log file: %v, falling back to stdout\n", err)

		f = os.Stdout
	}

	cmd := exec.Command("docker", "compose", "up", "-d")
	cmd.Dir = composeDir
	cmd.Stdout = f
	cmd.Stderr = f

	if err := cmd.Run(); err != nil {
		fmt.Printf("Error executing docker compose up: %v\n", err)
	}

	fmt.Println("docker compose up executed")
}

func dockerComposeDown(composeDir string) {
	cmd := exec.Command("docker", "compose", "down", "-v")
	cmd.Dir = composeDir

	if err := cmd.Run(); err != nil {
		fmt.Printf("Error executing docker compose down: %v\n", err)
	}

	fmt.Println("docker compose down executed")
}
