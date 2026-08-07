package framework

import (
	"fmt"
)

type UCLConfig struct {
	Flags     []string
	UclScript string
	Dir       string
	RpcUrl    string
}

type DBConfig struct {
	ComposeDir string
	Host       string
	Port       string
	User       string
	password   string
	Name       string
}

func (c *DBConfig) ConnString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.User, c.password, c.Host, c.Port, c.Name)
}

type SyncerConfig struct {
	RpcUrl            string
	Logging           bool
	PollInterval      uint64
	TipOnly           bool
	FullBlock         bool
	BatchSize         uint64
	TxWorkers         uint64
	Erc20Stats        bool
	Erc20StartFromTip bool
	EoaActivityStats  bool
	NumberOfRetries   uint64
	RetryInterval     uint64
}

type ApiConfig struct {
	Listen      string
	Logging     bool
	AdminSecret string
	NodeRPC     string
}

type TestClusterConfig struct {
	UCL     UCLConfig
	DB      DBConfig
	Syncer  SyncerConfig
	API     ApiConfig
	LogsDir string
	WithAPI bool
}

func DefaultFrameworkConfig() *TestClusterConfig {
	return &TestClusterConfig{
		UCL: UCLConfig{
			Flags:     []string{"write-logs"},
			UclScript: "scripts/cluster_syncer.sh",
			Dir:       "../ucl",
		},
		DB: DBConfig{
			ComposeDir: "../docker/db",
			Host:       "localhost",
			Port:       "5433",
			User:       "syncer",
			password:   "syncer",
			Name:       "syncer_e2e",
		},
		Syncer: SyncerConfig{
			RpcUrl:          "http://localhost:10002",
			PollInterval:    2000,
			BatchSize:       1,
			TxWorkers:       1,
			RetryInterval:   1000,
			NumberOfRetries: 10,
		},
		API: ApiConfig{
			Listen:      "localhost:8545",
			AdminSecret: "test-secret",
		},
	}
}
