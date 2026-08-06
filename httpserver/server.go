package httpserver

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/explorer"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/httpserver/publicapi"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/jsonrpc"
)

// Config matches polygon-edge jsonrpc.Config fields used for GET / (handleGetRequest).
// DB and AdminAPISecret enable POST /admin/v1/erc20/watchlist (Bearer token auth).
type Config struct {
	ChainName string
	ChainID   uint64
	Version   string
	DB        *sql.DB
	// AdminAPISecret: if empty, POST /admin/v1/erc20/watchlist returns 404 (set ADMIN_API_SECRET or --admin-api-secret).
	AdminAPISecret string
	NodeRPC        string
	// BalanceReader optionally injects a balance source for tests. When nil and NodeRPC is set,
	// New dials the node and constructs a node-backed reader.
	BalanceReader BalanceReader
}

// Server mirrors ucl-node2 jsonrpc.JSONRPC HTTP surface: POST / (JSON-RPC), GET /
// (chain metadata), /ws, plus GET /health for probes (not in polygon-edge but harmless).
type Server struct {
	handler              *jsonrpc.ExplorerHandler
	explorer             *explorer.Explorer
	cfg                  Config
	balanceReader        BalanceReader
	getBlockList         func(*api_storage.BlockListRequest) (interface{}, error)
	getTransactionByHash func(string) (*api_storage.TransactionListResponse, error)
	getTokenTransfers    func(api_storage.TokenTransfersRequest) (*api_storage.TokenTransfersResponse, error)
	getDailyCommitments  func(api_storage.DailyCommitmentsRequest) (*api_storage.DailyCommitmentsResponse, error)
}

// New creates the HTTP handler bundle. cfg supplies name/chain_id/version for GET / like polygon-edge.
func New(ex *explorer.Explorer, cfg Config) *Server {
	if cfg.Version == "" {
		cfg.Version = "0.0.1"
	}

	s := &Server{
		explorer: ex,
		cfg:      cfg,
		handler: &jsonrpc.ExplorerHandler{
			Explorer: ex,
		},
		balanceReader: cfg.BalanceReader,
	}

	if s.balanceReader == nil && strings.TrimSpace(cfg.NodeRPC) != "" {
		reader, err := NewNodeBalanceReader(cfg.NodeRPC)
		if err != nil {
			log.Printf("httpserver: failed to dial node RPC for balances: %v", err)
		} else {
			s.balanceReader = reader
		}
	}

	return s
}

// Handler returns the root http.Handler (polygon-edge: / and /ws; plus GET /health).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)

	// Public /api/v1 routes from OpenAPI (oapi-codegen). Independent of DB/admin auth.
	_ = publicapi.HandlerWithOptions(s, publicapi.StdHTTPServerOptions{
		BaseRouter:       mux,
		ErrorHandlerFunc: handlePublicAPIParamError,
	})
	mux.HandleFunc("/api/v1/data-anchor/daily-commitments", func(w http.ResponseWriter, _ *http.Request) {
		writePublicError(w, http.StatusMethodNotAllowed, "method_not_allowed", methodNotAllowed)
	})

	if s.cfg.DB != nil {
		mux.HandleFunc("POST /admin/v1/erc20/watchlist", s.handleAdminErc20Watchlist)
		mux.HandleFunc("/admin/v1/data-anchor/factories", s.handleAdminDataAnchorFactories)
		mux.HandleFunc("/admin/v1/validators/", s.handleAdminValidators)
		mux.HandleFunc("/admin/v1/asset-issuers/", s.handleAdminAssetIssuers)
		mux.HandleFunc("/admin/v1/asset-issuers", s.handleAdminAssetIssuers)
		mux.HandleFunc("/admin/v1/esg", s.handleAdminEsg)
	}

	mux.Handle("/", http.HandlerFunc(s.handle))
	mux.HandleFunc("/ws", s.handleWS)

	return middlewareFactory()(mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handle is the equivalent of polygon-edge (*JSONRPC).handle.
func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set(
		"Access-Control-Allow-Headers",
		"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization",
	)

	switch r.Method {
	case http.MethodPost:
		s.handleJSONRPCRequest(w, r)
	case http.MethodGet:
		s.handleGetRequest(w)
	case http.MethodOptions:
		// nothing to return (polygon-edge)
	default:
		_, _ = w.Write([]byte("method " + r.Method + " not allowed"))
	}
}

func (s *Server) handleJSONRPCRequest(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	resp, err := jsonrpc.HandleBody(s.handler, data)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
	} else {
		_, _ = w.Write(resp)
	}
}

// GetResponse matches polygon-edge jsonrpc.GetResponse (GET / body).
type GetResponse struct {
	Name    string `json:"name"`
	ChainID uint64 `json:"chain_id"`
	Version string `json:"version"`
}

func (s *Server) handleGetRequest(w http.ResponseWriter) {
	data := &GetResponse{
		Name:    s.cfg.ChainName,
		ChainID: s.cfg.ChainID,
		Version: s.cfg.Version,
	}

	resp, err := json.Marshal(data)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	_, _ = w.Write(resp)
}

// WebSocket is registered on polygon-edge but this service does not implement filter subscriptions.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "websocket not supported", http.StatusNotImplemented)
}

// middlewareFactory adds CORS headers on all routes (polygon-edge sets similar headers on `/`).
func middlewareFactory() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
			w.Header().Set(
				"Access-Control-Allow-Headers",
				"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization",
			)

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)

				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
