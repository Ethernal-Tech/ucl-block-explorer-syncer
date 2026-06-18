// e2e/framework/syncer.go
package framework

import (
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

type Syncer struct {
	node     *node
	config   SyncerConfig
	dbConfig DBConfig
	logsDir  string
	t        *testing.T
}

func NewSyncer(t *testing.T, cfg SyncerConfig, dbCfg DBConfig, logsDir string) *Syncer {
	t.Helper()

	return &Syncer{t: t, config: cfg, dbConfig: dbCfg, logsDir: logsDir}
}

func (s *Syncer) Start() {
	f, err := os.OpenFile(filepath.Join(s.logsDir, "syncer.log"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		s.t.Fatalf("failed to create syncer log file: %v", err)
	}

	n, err := newNode("go", s.buildArgs(), f, "..")
	if err != nil {
		s.t.Fatalf("failed to start syncer: %v", err)
	}

	s.node = n
}

func (s *Syncer) Stop() {
	if s.node == nil || s.node.cmd == nil {
		return
	}

	syscall.Kill(-s.node.cmd.Process.Pid, syscall.SIGTERM) //nolint:errcheck

	select {
	case <-s.node.Wait():
	case <-time.After(10 * time.Second):
		syscall.Kill(-s.node.cmd.Process.Pid, syscall.SIGKILL) //nolint:errcheck
	}
}

func (s *Syncer) IsRunning() bool {
	return s.node != nil && s.node.cmd != nil
}

func (s *Syncer) buildArgs() []string {
	args := []string{
		"run", ".", "sync",
		"--rpc-url", s.config.RpcUrl,
		"--db-conn", s.dbConfig.ConnString(),
		"--poll-interval", strconv.FormatUint(s.config.PollInterval, 10),
		"--batch-size", strconv.FormatUint(s.config.BatchSize, 10),
		"--tx-workers", strconv.FormatUint(s.config.TxWorkers, 10),
	}

	if s.config.Logging {
		args = append(args, "--logging")
	}

	if s.config.TipOnly {
		args = append(args, "--tip-only")
	}

	if s.config.FullBlock {
		args = append(args, "--full-block")
	}

	if s.config.Erc20Stats {
		args = append(args, "--erc20-stats")
		if s.config.Erc20StartFromTip {
			args = append(args, "--erc20-start-from-tip")
		}
	}

	if s.config.EoaActivityStats {
		args = append(args, "--eoa-activity-stats")
	}

	return args
}
