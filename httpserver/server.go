package httpserver

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/explorer"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/jsonrpc"
)

// Config matches polygon-edge jsonrpc.Config fields used for GET / (handleGetRequest).
type Config struct {
	ChainName string
	ChainID   uint64
	Version   string
}

// Server mirrors ucl-node2 jsonrpc.JSONRPC HTTP surface: POST / (JSON-RPC), GET /
// (chain metadata), /ws, plus GET /health for probes (not in polygon-edge but harmless).
type Server struct {
	handler  *jsonrpc.ExplorerHandler
	explorer *explorer.Explorer
	cfg      Config
}

// New creates the HTTP handler bundle. cfg supplies name/chain_id/version for GET / like polygon-edge.
func New(ex *explorer.Explorer, cfg Config) *Server {
	if cfg.Version == "" {
		cfg.Version = "0.0.1"
	}
	return &Server{
		explorer: ex,
		cfg:      cfg,
		handler: &jsonrpc.ExplorerHandler{
			Explorer: ex,
		},
	}
}

// Handler returns the root http.Handler (polygon-edge: / and /ws; plus GET /health).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
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
