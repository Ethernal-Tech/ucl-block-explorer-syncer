package cli

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:          "ucl-block-explorer-syncer",
	Short:        "Index EVM chain data into PostgreSQL and serve the explorer JSON-RPC API.",
	SilenceUsage: true,
}

func init() {
	rootCmd.AddCommand(syncerCommand, apiCommand)
}

// Execute runs the root command (sync or api subcommand).
func Execute() {
	setRequiredFlags()
	setOptionalFlags()

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
