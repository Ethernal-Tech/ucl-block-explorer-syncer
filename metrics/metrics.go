// Package metrics provides the Prometheus instrumentation for the block-explorer
// syncer.
//
// This is a metrics-first baseline: no OpenTelemetry/tracing this pass (deferred
// until the node moves off dormant DataDog onto OTel). Instruments are registered
// on a dedicated registry and exposed through [Handler], mirroring the dedicated
// metrics-listener convention used by ucl-prover / ucl-zk-server. The syncer
// updates the exported instruments directly; a lightweight HTTP server (started
// from the run path) serves them at /metrics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// registry is a dedicated registry for the syncer's metrics. Using a private
// registry (rather than the global default) keeps the exposed series explicit
// and avoids collisions with any third-party default-registry instrumentation.
var registry = prometheus.NewRegistry()

// factory registers every instrument it creates on the dedicated registry.
var factory = promauto.With(registry)

// Queue label values for [QueueDepth]. Each names one inter-worker channel in
// the ingest pipeline.
const (
	// QueueBlockCache is the in-memory list buffering blocks produced by the
	// block worker until the tx workers consume them.
	QueueBlockCache = "block_cache"
	// QueueTxJobs is the (summed) backlog of dispatched-but-unstarted tx jobs
	// across all tx worker job channels.
	QueueTxJobs = "tx_jobs"
	// QueueTxDone is the backlog of completion signals from the tx workers
	// waiting to be drained by the dispatcher.
	QueueTxDone = "tx_done"
)

var (
	// ChainHeadBlock is the latest block number reported by the node.
	ChainHeadBlock = factory.NewGauge(prometheus.GaugeOpts{
		Name: "syncer_chain_head_block",
		Help: "Latest block number reported by the node (chain head).",
	})

	// LastIndexedBlock is the last block fully indexed (block + transactions
	// persisted) by the syncer.
	LastIndexedBlock = factory.NewGauge(prometheus.GaugeOpts{
		Name: "syncer_last_indexed_block",
		Help: "Last block number fully indexed by the syncer.",
	})

	// IndexingLagBlocks is the #1 health signal: how many blocks behind the
	// chain head the syncer is (chainHead - lastIndexed, floored at 0).
	IndexingLagBlocks = factory.NewGauge(prometheus.GaugeOpts{
		Name: "syncer_indexing_lag_blocks",
		Help: "Blocks the syncer is behind the chain head (chainHead - lastIndexed).",
	})

	// BlocksProcessed counts blocks fully processed (indexed) by the syncer.
	BlocksProcessed = factory.NewCounter(prometheus.CounterOpts{
		Name: "syncer_blocks_processed_total",
		Help: "Total number of blocks fully processed (indexed) by the syncer.",
	})

	// TxsProcessed counts transactions processed (indexed) by the syncer. The
	// empty-block sentinel transaction is not counted.
	TxsProcessed = factory.NewCounter(prometheus.CounterOpts{
		Name: "syncer_txs_processed_total",
		Help: "Total number of transactions processed (indexed) by the syncer.",
	})

	// QueueDepth reports the current depth of each inter-worker queue/channel,
	// labelled by queue (see the Queue* constants). A queue trending toward its
	// capacity points at where the pipeline is back-pressured.
	QueueDepth = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "syncer_queue_depth",
		Help: "Current depth of an inter-worker queue/channel, by queue.",
	}, []string{"queue"})

	// NodeRPCDuration times JSON-RPC calls to the node, labelled by method, so
	// node latency can be attributed and separated from syncer-side work.
	NodeRPCDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "syncer_node_rpc_duration_seconds",
		Help:    "Duration of JSON-RPC calls to the node, by method.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12), // ~5ms .. ~10s
	}, []string{"method"})
)

func init() {
	// Go runtime + process collectors give CPU/memory/GC/FD visibility for free
	// on the dedicated registry (the default registry's collectors are not used
	// here because we serve from our own registry).
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
