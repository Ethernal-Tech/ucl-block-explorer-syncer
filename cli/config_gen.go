package cli

import (
	"fmt"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/utils"
	"github.com/spf13/cobra"
)

var (
	cfgOut                   string
	cfgFilterRegions         []string
	cfgFilterServices        []string
	cfgFilterUsageAccountIds []string
)

var genConfigCommand = &cobra.Command{
	Use:   "gen-config",
	Short: "Generate JSON config containing api and syncer mappings",
	RunE:  runGenConfig,
}

func init() {
	genConfigCommand.Flags().StringVarP(&cfgOut, "output", "o", "config.json", "output file path")
	genConfigCommand.Flags().StringSliceVar(
		&cfgFilterRegions, "esg-filter-regions", nil, "ESG filter region (repeatable)")
	genConfigCommand.Flags().StringSliceVar(
		&cfgFilterServices, "esg-filter-services", nil, "ESG filter service (repeatable)")
	genConfigCommand.Flags().StringSliceVar(
		&cfgFilterUsageAccountIds, "esg-filter-usage-account-ids", nil, "ESG filter usage account id (repeatable)")
}

func runGenConfig(cmd *cobra.Command, args []string) error {
	esg := &utils.ESGAggregationBackendConfig{
		FilterRegions:         cfgFilterRegions,
		FilterServices:        cfgFilterServices,
		FilterUsageAccountIds: cfgFilterUsageAccountIds,
	}

	cfg := utils.ConfigFile{
		API: &utils.APIConfig{},
		Syncer: &utils.SyncerConfig{
			ESG: esg,
		},
	}

	if err := cfg.Save(cfgOut); err != nil {
		return err
	}

	fmt.Printf("Wrote config to %s\n", cfgOut)

	return nil
}
