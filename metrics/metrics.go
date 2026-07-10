// Package metrics provides Prometheus instrumentation for the block-explorer
// syncer. Instruments are registered on a dedicated registry and exposed
// through [Handler] at /metrics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// registry is a dedicated Prometheus registry; keeps exposed series explicit
// and avoids collisions with third-party default-registry instrumentation.
var registry = prometheus.NewRegistry()

var factory = promauto.With(registry)

// Queue label values for [QueueDepth]. Each names one inter-worker channel in
// the ingest pipeline.
const (
	QueueBlockCache = "block_cache"
	QueueTxJobs     = "tx_jobs"
)

var (
	ChainHeadBlock = factory.NewGauge(prometheus.GaugeOpts{
		Name: "syncer_chain_head_block",
		Help: "Latest block number reported by the node (chain head).",
	})

	LastIndexedBlock = factory.NewGauge(prometheus.GaugeOpts{
		Name: "syncer_last_indexed_block",
		Help: "Last block number fully indexed by the syncer.",
	})

	// IndexingLagBlocks is floored at 0 (chainHead - lastIndexed).
	IndexingLagBlocks = factory.NewGauge(prometheus.GaugeOpts{
		Name: "syncer_indexing_lag_blocks",
		Help: "Blocks the syncer is behind the chain head (chainHead - lastIndexed).",
	})

	BlocksProcessed = factory.NewCounter(prometheus.CounterOpts{
		Name: "syncer_blocks_processed_total",
		Help: "Total number of blocks fully processed (indexed) by the current syncer run " +
			"(excludes blocks indexed by previous runs).",
	})

	// TxsProcessed does not count the empty-block sentinel transaction.
	TxsProcessed = factory.NewCounter(prometheus.CounterOpts{
		Name: "syncer_txs_processed_total",
		Help: "Total number of transactions processed (indexed) by the current syncer run " +
			"(excludes transactions indexed by previous runs).",
	})

	// QueueDepth labelled by queue (see Queue* constants); a queue trending
	// toward capacity indicates back-pressure at that pipeline stage.
	QueueDepth = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "syncer_queue_depth",
		Help: "Current depth of an inter-worker queue/channel, by queue.",
	}, []string{"queue"})

	NodeRPCDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "syncer_node_rpc_duration_seconds",
		Help:    "Duration of JSON-RPC calls to the node, by method.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12), // ~5ms .. ~10s
	}, []string{"method"})
)

func init() {
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
}

// Handler returns an http.Handler that serves the syncer metrics in Prometheus
// text exposition format from the dedicated registry.
func Handler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}
