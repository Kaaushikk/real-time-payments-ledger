package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func init() {
	for _, result := range []string{"posted", "replayed", "invalid", "error"} {
		Transfers.WithLabelValues(result)
	}
	for _, result := range []string{"success", "error"} {
		PublishBatches.WithLabelValues(result)
	}
	_ = prometheus.Register(collectors.NewGoCollector())
	_ = prometheus.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

var (
	Transfers = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_transfers_total",
		Help: "Transfer service requests by outcome.",
	}, []string{"result"})
	TransferLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ledger_transfer_duration_seconds",
		Help:    "Transfer service call duration, including retries.",
		Buckets: prometheus.DefBuckets,
	})
)

var (
	PublishBatches = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_outbox_publish_batches_total",
		Help: "Nonempty outbox publication batches by outcome.",
	}, []string{"result"})
	PublishLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ledger_outbox_publish_duration_seconds",
		Help:    "Nonempty outbox batch publication duration through database acknowledgement.",
		Buckets: prometheus.DefBuckets,
	})
)
