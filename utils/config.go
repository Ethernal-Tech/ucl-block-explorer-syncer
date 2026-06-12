package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

const (
	ESGConfigKey = "esg"
	permissions  = 0644
)

// ESGAggregationBackendConfig holds filtering options for the ESG backend.
type ESGAggregationBackendConfig struct {
	FilterRegions         []string `json:"filter_regions"`
	FilterServices        []string `json:"filter_services"`
	FilterUsageAccountIds []string `json:"filter_usage_account_ids"`
}

type APIConfig struct {
	// add API config fields here in the future
}

type SyncerConfig struct {
	ESG *ESGAggregationBackendConfig `json:"esg,omitempty"`
}

type ConfigFile struct {
	API    *APIConfig    `json:"api,omitempty"`
	Syncer *SyncerConfig `json:"syncer,omitempty"`
}

func (cfg *ConfigFile) Save(cfgOut string) error {
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(cfgOut, b, permissions); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

func LoadConfig(filePath string) (*ConfigFile, error) {
	if filePath == "" {
		return &ConfigFile{}, nil // empty config if no path provided
	}

	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("config file does not exist at path: %s", filePath)
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg *ConfigFile

	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse esg config file: %w", err)
	}

	return cfg, nil
}
