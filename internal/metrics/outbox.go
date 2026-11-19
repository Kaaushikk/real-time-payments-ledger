package metrics

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// OutboxCollector reads current backlog state on each scrape.
type OutboxCollector struct {
	pool    *pgxpool.Pool
	pending *prometheus.Desc
	oldest  *prometheus.Desc
	success *prometheus.Desc
}

func NewOutboxCollector(pool *pgxpool.Pool) *OutboxCollector {
	return &OutboxCollector{
		pool:    pool,
		pending: prometheus.NewDesc("ledger_outbox_pending_events", "Committed events waiting for publication.", nil, nil),
		oldest:  prometheus.NewDesc("ledger_outbox_oldest_pending_age_seconds", "Age of the oldest pending event, or zero for an empty outbox.", nil, nil),
		success: prometheus.NewDesc("ledger_outbox_collection_success", "Whether the latest outbox database query succeeded.", nil, nil),
	}
}

func (c *OutboxCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.pending
	ch <- c.oldest
	ch <- c.success
}

func (c *OutboxCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var pending int64
	var oldest float64
	err := c.pool.QueryRow(ctx, `
		SELECT count(*),
		       COALESCE(GREATEST(EXTRACT(EPOCH FROM (clock_timestamp() - min(created_at))), 0), 0)::double precision
		FROM outbox WHERE published_at IS NULL
	`).Scan(&pending, &oldest)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.success, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.success, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(c.pending, prometheus.GaugeValue, float64(pending))
	ch <- prometheus.MustNewConstMetric(c.oldest, prometheus.GaugeValue, oldest)
}
