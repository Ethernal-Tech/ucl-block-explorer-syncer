package cli

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:          "ucl-block-explorer-syncer",
	Short:        "Index EVM chain data into PostgreSQL and serve the explorer JSON-RPC API.",
	SilenceUsage: true,

	// Cobra would otherwise print the error itself as bare text. Execute logs it as a
	// structured record instead, so a failed start-up reaches log shipping looking like
	// every other line rather than being the one event that does not.
	SilenceErrors: true,
}

func init() {
	rootCmd.AddCommand(syncerCommand, apiCommand, genConfigCommand)
}

// Execute runs the root command (sync or api subcommand).
func Execute() {
	setRequiredFlags()
	setOptionalFlags()

	if err := rootCmd.Execute(); err != nil {
		// slog.Default is whatever the subcommand configured; if it failed before
		// getting that far, this is the standard library's handler, which still prints.
		slog.Error("start-up failed", "error", err.Error())
		os.Exit(1)
	}
}
