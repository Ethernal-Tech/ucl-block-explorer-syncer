package framework

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

var SharedDB *DB

type TestCluster struct {
	Config   *TestClusterConfig
	UCL      *UCL
	DB       *DB
	Syncer   *Syncer
	API      *API
	t        *testing.T
	sharedDB bool
	doneCh   chan struct{}
}

type Option func(*TestClusterConfig)

func WithUclFlags(flags ...string) Option {
	return func(cfg *TestClusterConfig) {
		cfg.UCL.Flags = flags
	}
}

func WithErc20Stats() Option {
	return func(cfg *TestClusterConfig) {
		cfg.Syncer.Erc20Stats = true
	}
}

func WithEoaActivity() Option {
	return func(cfg *TestClusterConfig) {
		cfg.Syncer.EoaActivityStats = true
	}
}

func WithFullBlock() Option {
	return func(cfg *TestClusterConfig) {
		cfg.Syncer.FullBlock = true
	}
}

func WithTipOnly() Option {
	return func(cfg *TestClusterConfig) {
		cfg.Syncer.TipOnly = true
	}
}

func WithLogging() Option {
	return func(cfg *TestClusterConfig) {
		cfg.Syncer.Logging = true
	}
}

func WithBatchSize(size uint64) Option {
	return func(cfg *TestClusterConfig) {
		cfg.Syncer.BatchSize = size
	}
}

func WithTxWorkers(workers uint64) Option {
	return func(cfg *TestClusterConfig) {
		cfg.Syncer.TxWorkers = workers
	}
}

func WithErc20StartFromTip() Option {
	return func(cfg *TestClusterConfig) {
		cfg.Syncer.Erc20StartFromTip = true
	}
}

func WithAPILogging() Option {
	return func(cfg *TestClusterConfig) {
		cfg.API.Logging = true
	}
}

func WithAPIListen(addr string) Option {
	return func(cfg *TestClusterConfig) {
		cfg.API.Listen = addr
	}
}

func WithAdminSecret(secret string) Option {
	return func(cfg *TestClusterConfig) {
		cfg.API.AdminSecret = secret
	}
}

func WithAPI() Option {
	return func(cfg *TestClusterConfig) {
		cfg.WithAPI = true
	}
}

func NewTestCluster(t *testing.T, opts ...Option) *TestCluster {
	t.Helper()

	cfg := DefaultFrameworkConfig()

	for _, opt := range opts {
		opt(cfg)
	}

	tc := &TestCluster{
		t:      t,
		Config: cfg,
	}

	tc.initLogsDir()

	// use shared DB if available, otherwise start own
	if SharedDB != nil {
		tc.DB = SharedDB
		tc.DB.TruncateAll(t)
		tc.sharedDB = true
	} else {
		tc.DB = NewDB(cfg.DB, cfg.LogsDir)
	}

	tc.UCL = NewUCL(t, cfg.UCL, cfg.LogsDir)
	tc.Syncer = NewSyncer(t, cfg.Syncer, cfg.DB, cfg.LogsDir)

	if cfg.WithAPI {
		tc.API = NewAPI(t, cfg.API, cfg.DB, cfg.LogsDir)
	}

	tc.doneCh = make(chan struct{})

	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case <-sigCh:
			tc.Stop()
			os.Exit(1)
		case <-tc.doneCh:
		}
	}()

	t.Cleanup(func() {
		close(tc.doneCh)
		signal.Stop(sigCh)
	})

	return tc
}

func (tc *TestCluster) Start() {
	tc.UCL.Start()
	tc.Syncer.Start()

	if tc.Config.WithAPI {
		tc.API.Start()
	}
}

func (tc *TestCluster) Stop() {
	if tc.Config.WithAPI {
		tc.API.Stop()
	}

	tc.Syncer.Stop()
	tc.UCL.Stop()

	if !tc.sharedDB {
		tc.DB.Stop()
	}
}

func (tc *TestCluster) initLogsDir() {
	name := tc.t.Name()
	parent := name
	sub := ""

	if p, s, ok := strings.Cut(name, "/"); ok {
		parent = p
		sub = s
	}

	dir := filepath.Join("../e2e-logs", fmt.Sprintf("%s-%d", parent, time.Now().UTC().UnixMilli()))

	if sub != "" {
		dir = filepath.Join(dir, sub)
	}

	if err := os.MkdirAll(dir, 0750); err != nil {
		tc.t.Fatalf("failed to create logs dir: %v", err)
	}

	tc.Config.LogsDir = dir
	tc.t.Logf("logs dir: %s", dir)
}

func (tc *TestCluster) RestartSyncer(newRpcUrl string) {
	tc.Syncer.config.RpcUrl = newRpcUrl
	tc.Syncer.Start()
}
