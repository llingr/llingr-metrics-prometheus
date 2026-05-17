// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-metrics-prometheus Authors
// SPDX-License-Identifier: Apache-2.0

// Package messages provides a Prometheus-compatible MetricsSink implementation
// for the llingr message processing framework.
package messages

import (
	"net/http"
	"strconv"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Sink collects llingr framework metrics and exposes them for Prometheus scraping.
type Sink struct {
	registry *prometheus.Registry

	// counters
	messagesProcessed  *prometheus.CounterVec
	messagesErrored    *prometheus.CounterVec
	messagesPanicked   *prometheus.CounterVec
	messagesDeadLetter *prometheus.CounterVec
	messagesDuplicate  *prometheus.CounterVec
	messagesOverflow   *prometheus.CounterVec

	// gauges
	queueDepth    *prometheus.GaugeVec
	currentOffset *prometheus.GaugeVec

	// histograms
	processDuration    *prometheus.HistogramVec
	deadLetterDuration *prometheus.HistogramVec
	queueWaitDuration  *prometheus.HistogramVec
}

// New creates a new Prometheus metrics sink with its own registry.
// Use RegisterHandler() to expose metrics at a path chosen by the host application.
// See options.go for available Option constructors.
func New(opts ...Option) *Sink {
	o := processOptions(opts...)
	s := &Sink{registry: prometheus.NewRegistry()}

	labels := []string{"topic", "consumer_group", "application", "team", "partition"}

	s.messagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "processed_total",
			Help:      "Total number of messages processed",
		},
		labels,
	)

	s.messagesErrored = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "errored_total",
			Help:      "Total number of messages that resulted in processing errors",
		},
		labels,
	)

	s.messagesPanicked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "panicked_total",
			Help:      "Total number of messages where processing panicked",
		},
		labels,
	)

	s.messagesDeadLetter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "dead_lettered_total",
			Help:      "Total number of messages sent to dead letter queue",
		},
		labels,
	)

	s.messagesDuplicate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "duplicate_total",
			Help:      "Total number of duplicate messages detected",
		},
		labels,
	)

	s.messagesOverflow = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "used_overflow_total",
			Help:      "Total messages that used guard channel overflow during worker acquisition",
		},
		labels,
	)

	s.queueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "queue_depth",
			Help:      "Current queue depth for buffering implementations",
		},
		labels,
	)

	s.currentOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "current_offset",
			Help:      "Current offset being processed per partition",
		},
		labels,
	)

	s.processDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "process_duration_seconds",
			Help:      "Time spent processing messages",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
		},
		labels,
	)

	s.deadLetterDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "dead_letter_duration_seconds",
			Help:      "Time spent writing to dead letter queue",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
		},
		labels,
	)

	s.queueWaitDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "queue_wait_duration_seconds",
			Help:      "Time messages spent waiting in queue before processing",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 18), // 0.1ms to ~13s
		},
		labels,
	)

	s.registry.MustRegister(
		s.messagesProcessed,
		s.messagesErrored,
		s.messagesPanicked,
		s.messagesDeadLetter,
		s.messagesDuplicate,
		s.messagesOverflow,
		s.queueDepth,
		s.currentOffset,
		s.processDuration,
		s.deadLetterDuration,
		s.queueWaitDuration,
	)

	return s
}

// teamName returns the team name from a SinkContext, or "" if no team was
// configured via WithTeam(). Empty string is a valid Prometheus label value
// and lets dashboards filter out untagged consumers cleanly.
func teamName(team *nexus.Team) string {
	if team == nil {
		return ""
	}
	return team.Name
}

// MetricsSink returns a nexus.MetricsSink function that records metrics to Prometheus.
func (s *Sink) MetricsSink() nexus.MetricsSink {
	return func(ctx nexus.SinkContext, metrics nexus.Metrics) error {
		partition := strconv.FormatInt(int64(metrics.Partition), 10)
		labels := prometheus.Labels{
			"topic":          ctx.TopicName,
			"consumer_group": ctx.ConsumerGroup,
			"application":    ctx.ApplicationName,
			"team":           teamName(ctx.Team),
			"partition":      partition,
		}

		// always increment processed counter
		s.messagesProcessed.With(labels).Inc()

		// increment trait-based counters
		if metrics.Traits&nexus.ProcessError != 0 {
			s.messagesErrored.With(labels).Inc()
		}
		if metrics.Traits&nexus.ProcessPanic != 0 {
			s.messagesPanicked.With(labels).Inc()
		}
		if metrics.Traits&nexus.DeadLetter != 0 {
			s.messagesDeadLetter.With(labels).Inc()
		}
		if metrics.Traits&nexus.Duplicate != 0 {
			s.messagesDuplicate.With(labels).Inc()
		}
		if metrics.Traits&nexus.UsedOverflow != 0 {
			s.messagesOverflow.With(labels).Inc()
		}

		// update gauges
		s.queueDepth.With(labels).Set(float64(metrics.QueueDepth))
		s.currentOffset.With(labels).Set(float64(metrics.Offset))

		// record durations as histograms
		if metrics.ProcessDuration > 0 {
			s.processDuration.With(labels).Observe(metrics.ProcessDuration.Seconds())
		}
		if metrics.WriteDeadLetterDuration > 0 {
			s.deadLetterDuration.With(labels).Observe(metrics.WriteDeadLetterDuration.Seconds())
		}

		// queue wait = time from read to process start
		if !metrics.ReadTime.IsZero() && !metrics.ProcessStartTime.IsZero() {
			queueWait := metrics.ProcessStartTime.Sub(metrics.ReadTime)
			if queueWait > 0 {
				s.queueWaitDuration.With(labels).Observe(queueWait.Seconds())
			}
		}

		return nil
	}
}

// RegisterHandler registers the Prometheus metrics handler at the specified path
// on the provided ServeMux. The host application controls where metrics are exposed.
//
// Example:
//
//	mux := http.NewServeMux()
//	sink.RegisterHandler(mux, "/metrics")
//	http.ListenAndServe(":8080", mux)
func (s *Sink) RegisterHandler(mux *http.ServeMux, path string) {
	handler := promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
		Timeout:           10 * time.Second,
	})
	mux.Handle(path, handler)
}

// Registry returns the underlying Prometheus registry for advanced use cases.
func (s *Sink) Registry() *prometheus.Registry {
	return s.registry
}
