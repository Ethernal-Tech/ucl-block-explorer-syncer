package e2e

import (
	"os"
	"testing"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
)

func TestMain(m *testing.M) {
	cfg := framework.DefaultFrameworkConfig()

	framework.SharedDB = framework.NewDB(nil, cfg.DB, ".")
	framework.SharedDB.StartForTestMain()

	code := m.Run()

	framework.SharedDB.Stop()
	os.Exit(code)
}
