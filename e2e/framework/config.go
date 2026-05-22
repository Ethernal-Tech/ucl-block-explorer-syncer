package framework

import "fmt"

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
	Password   string
	Name       string
}

func (c *DBConfig) ConnString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.User, c.Password, c.Host, c.Port, c.Name)
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
}

type TestClusterConfig struct {
	UCL     UCLConfig
	DB      DBConfig
	Syncer  SyncerConfig
	LogsDir string
}

func DefaultFrameworkConfig() *TestClusterConfig {
	return &TestClusterConfig{
		UCL: UCLConfig{
			Flags:     []string{"write-logs"},
			UclScript: "scripts/cluster_syncer.sh",
			Dir:       "../ucl",
			RpcUrl:    "http://localhost:10002",
		},
		DB: DBConfig{
			ComposeDir: "../docker/db",
			Host:       "localhost",
			Port:       "5433",
			User:       "syncer",
			Password:   "syncer",
			Name:       "syncer_e2e",
		},
		Syncer: SyncerConfig{
			RpcUrl:       "http://localhost:10002",
			PollInterval: 2000,
			BatchSize:    1,
			TxWorkers:    1,
		},
	}
}
