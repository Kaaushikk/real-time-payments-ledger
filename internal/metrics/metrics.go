package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func init() {
	_ = prometheus.Register(collectors.NewGoCollector())
	_ = prometheus.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

var (
	Transfers = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_transfers_total",
		Help: "Posted transfers by outcome.",
	}, []string{"result"})
	TransferLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ledger_transfer_duration_seconds",
		Help:    "Transfer handler latency.",
		Buckets: prometheus.DefBuckets,
	})
)
