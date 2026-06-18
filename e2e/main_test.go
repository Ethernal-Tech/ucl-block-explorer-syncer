package e2e

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
)

func TestMain(m *testing.M) {
	logsDir := filepath.Join("../e2e-logs", fmt.Sprintf("sharedDB-%d", time.Now().UTC().UnixMilli()))
	if err := os.MkdirAll(logsDir, 0750); err != nil {
		fmt.Printf("failed to create logs dir: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("shared logs dir: %s\n", logsDir)

	cfg := framework.DefaultFrameworkConfig()

	framework.SharedDB = framework.NewDB(cfg.DB, logsDir)
	framework.SharedDB.StartForTestMain()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		framework.SharedDB.Stop()
		os.Exit(1)
	}()

	code := m.Run()

	framework.SharedDB.Stop()
	os.Exit(code)
}
