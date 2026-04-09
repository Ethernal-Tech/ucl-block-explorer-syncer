package cli

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/explorer"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/httpserver"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
)

var (
	apiListen         string
	apiDBConn         string
	apiLogging        bool
	apiChainName      string
	apiChainID        uint64
	apiVersion        string
	apiAdminAPISecret string
)

var apiCommand = &cobra.Command{
	Use:   "api",
	Short: "Serve explorer JSON-RPC on POST / (same HTTP layout as polygon-edge ucl-node2).",
	Long: `HTTP surface matches polygon-edge jsonrpc.JSONRPC:

  POST /     — JSON-RPC 2.0 (explorer_* methods)
  GET /      — { "name", "chain_id", "version" } (same shape as polygon-edge GET /)
  /ws        — registered; returns 501 (filters/subscriptions not implemented here)

Optional: POST /admin/v1/erc20/watchlist — register tokens in chain.erc20_watchlist (Bearer ADMIN_API_SECRET).

There are no /api/... REST routes on the node; use POST / with explorer_* methods.`,
	RunE: runAPI,
}

func init() {
	apiCommand.Flags().StringVarP(&apiListen, "listen", "l", "0.0.0.0:8545",
		"TCP listen address (same default role as polygon-edge JSON-RPC port)")
	apiCommand.Flags().StringVarP(&apiDBConn, "db-conn", "c", "",
		"[REQUIRED] PostgreSQL connection string (same DB as the syncer)")
	apiCommand.Flags().BoolVarP(&apiLogging, "logging", "v", false,
		"enable explorer handler logging")
	apiCommand.Flags().StringVar(&apiChainName, "chain-name", "",
		"value for GET / JSON field \"name\" (polygon-edge: ChainName)")
	apiCommand.Flags().Uint64Var(&apiChainID, "chain-id", 0,
		"value for GET / JSON field \"chain_id\"")
	apiCommand.Flags().StringVar(&apiVersion, "version", "0.0.1",
		"value for GET / JSON field \"version\"")
	apiCommand.Flags().StringVar(&apiAdminAPISecret, "admin-api-secret", "",
		"Bearer token for POST /admin/v1/erc20/watchlist (default: ADMIN_API_SECRET env)")
	_ = apiCommand.MarkFlagRequired("db-conn")
}

func runAPI(cmd *cobra.Command, args []string) error {
	db, err := sql.Open("postgres", apiDBConn)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("db ping: %w", err)
	}

	api_storage.SetDB(db)

	ex := explorer.NewExplorer()
	if apiLogging {
		ex.Logger = func(format string, a ...any) {
			log.Printf(format, a...)
		}
	}

	adminSecret := os.Getenv("ADMIN_API_SECRET")
	if apiAdminAPISecret != "" {
		adminSecret = apiAdminAPISecret
	}

	srv := httpserver.New(ex, httpserver.Config{
		ChainName:      apiChainName,
		ChainID:        apiChainID,
		Version:        apiVersion,
		DB:             db,
		AdminAPISecret: adminSecret,
	})
	log.Printf("explorer API listening on %s (POST / JSON-RPC; GET / metadata — polygon-edge compatible)", apiListen)

	return http.ListenAndServe(apiListen, srv.Handler())
}
