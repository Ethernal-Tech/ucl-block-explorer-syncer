package framework

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

type TestCluster struct {
	Config *TestClusterConfig
	UCL    *UCL
	DB     *DB
	Syncer *Syncer
	t      *testing.T
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

func NewTestCluster(t *testing.T, opts ...Option) *TestCluster {
	t.Helper()

	cfg := DefaultFrameworkConfig()

	for _, opt := range opts {
		opt(cfg)
	}

	fw := &TestCluster{
		t:      t,
		Config: cfg,
	}

	fw.initLogsDir()

	fw.UCL = NewUCL(t, cfg.UCL, cfg.LogsDir)
	fw.DB = NewDB(t, cfg.DB, cfg.LogsDir)
	fw.Syncer = NewSyncer(t, cfg.Syncer, cfg.DB, cfg.LogsDir)

	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fw.Stop()
		os.Exit(1)
	}()

	return fw
}

func (fw *TestCluster) Start() {
	fw.DB.Start()
	fw.UCL.Start()
	fw.Syncer.Start()
}

func (fw *TestCluster) Stop() {
	fw.Syncer.Stop()
	fw.UCL.Stop()
	fw.DB.Stop()
}

func (fw *TestCluster) initLogsDir() {
	dir := filepath.Join("../e2e-logs", fmt.Sprintf("%s-%d", fw.t.Name(), time.Now().UTC().UnixMilli()))

	if err := os.MkdirAll(dir, 0750); err != nil {
		fw.t.Fatalf("failed to create logs dir: %v", err)
	}

	fw.Config.LogsDir = dir
	fw.t.Logf("logs dir: %s", dir)
}
