// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-metrics-prometheus Authors
// SPDX-License-Identifier: Apache-2.0

// Package messages provides a Prometheus-compatible MetricsSink implementation
// for the llingr message processing framework.
package messages

import (
	"net/http"
	"strconv"
	"sync"
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

	// the metrics recorded for every message, resolved once per set of label values; the
	// histograms only on their first observation, so one that is never observed is never
	// exported
	seriesMu          sync.RWMutex
	series            map[seriesKey]*series
	processDurationBy map[seriesKey]prometheus.Observer
	queueWaitBy       map[seriesKey]prometheus.Observer
}

// seriesKey is one set of label values. A struct of strings is looked up in a map without
// allocating, where a prometheus.Labels map would be built and hashed for every message.
type seriesKey struct {
	topic, consumerGroup, service, team string
	partition                           int32
}

// series holds the children recorded for every message. The rarer ones (errors, panics,
// dead letters, duplicates, overflow) are still resolved when they happen, so a series that
// never occurs is never exported, as before.
type series struct {
	processed     prometheus.Counter
	queueDepth    prometheus.Gauge
	currentOffset prometheus.Gauge
}

func (k seriesKey) labels() prometheus.Labels {
	return prometheus.Labels{
		"topic":          k.topic,
		"consumer_group": k.consumerGroup,
		"service":        k.service,
		"team":           k.team,
		"partition":      strconv.FormatInt(int64(k.partition), 10),
	}
}

func (s *Sink) seriesFor(key seriesKey) *series {
	s.seriesMu.RLock()
	sr := s.series[key]
	s.seriesMu.RUnlock()
	if sr != nil {
		return sr
	}

	labels := key.labels()
	sr = &series{
		processed:     s.messagesProcessed.With(labels),
		queueDepth:    s.queueDepth.With(labels),
		currentOffset: s.currentOffset.With(labels),
	}
	s.seriesMu.Lock()
	if existing := s.series[key]; existing != nil {
		sr = existing
	} else {
		s.series[key] = sr
	}
	s.seriesMu.Unlock()
	return sr
}

// observerFor returns vec's child for key from cache, resolving it on first use.
func (s *Sink) observerFor(cache map[seriesKey]prometheus.Observer, vec *prometheus.HistogramVec,
	key seriesKey) prometheus.Observer {
	s.seriesMu.RLock()
	o := cache[key]
	s.seriesMu.RUnlock()
	if o != nil {
		return o
	}

	o = vec.With(key.labels())
	s.seriesMu.Lock()
	if existing := cache[key]; existing != nil {
		o = existing
	} else {
		cache[key] = o
	}
	s.seriesMu.Unlock()
	return o
}

// New creates a new Prometheus metrics sink with its own registry.
// Use RegisterHandler() to expose metrics at a path chosen by the host application.
// See options.go for available Option constructors.
func New(opts ...Option) *Sink {
	o := processOptions(opts...)
	s := &Sink{
		registry:          prometheus.NewRegistry(),
		series:            make(map[seriesKey]*series),
		processDurationBy: make(map[seriesKey]prometheus.Observer),
		queueWaitBy:       make(map[seriesKey]prometheus.Observer),
	}

	labels := []string{"topic", "consumer_group", "service", "team", "partition"}

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

// serviceLabels resolves the service and team labels from SinkContext.Service.
// Empty strings are valid Prometheus label values and let dashboards filter
// untagged consumers cleanly
func serviceLabels(service *nexus.Service) (svc, team string) {
	if service == nil {
		return "", ""
	}
	return service.Name, service.Team
}

// MetricsSink returns a nexus.MetricsSink function that records metrics to Prometheus.
func (s *Sink) MetricsSink() nexus.MetricsSink {
	return func(ctx nexus.SinkContext, metrics nexus.Metrics) error {
		svc, team := serviceLabels(ctx.Service)
		key := seriesKey{
			topic:         ctx.TopicName,
			consumerGroup: ctx.ConsumerGroup,
			service:       svc,
			team:          team,
			partition:     metrics.Partition,
		}
		sr := s.seriesFor(key)

		// always increment processed counter
		sr.processed.Inc()

		// increment trait-based counters
		if metrics.Traits&nexus.ProcessError != 0 {
			s.messagesErrored.With(key.labels()).Inc()
		}
		if metrics.Traits&nexus.ProcessPanic != 0 {
			s.messagesPanicked.With(key.labels()).Inc()
		}
		if metrics.Traits&nexus.DeadLetter != 0 {
			s.messagesDeadLetter.With(key.labels()).Inc()
		}
		if metrics.Traits&nexus.Duplicate != 0 {
			s.messagesDuplicate.With(key.labels()).Inc()
		}
		if metrics.Traits&nexus.UsedOverflow != 0 {
			s.messagesOverflow.With(key.labels()).Inc()
		}

		// update gauges
		sr.queueDepth.Set(float64(metrics.QueueDepth))
		sr.currentOffset.Set(float64(metrics.Offset))

		// record durations as histograms
		if metrics.ProcessDuration > 0 {
			s.observerFor(s.processDurationBy, s.processDuration, key).
				Observe(metrics.ProcessDuration.Seconds())
		}
		if metrics.WriteDeadLetterDuration > 0 {
			s.deadLetterDuration.With(key.labels()).Observe(metrics.WriteDeadLetterDuration.Seconds())
		}

		// queue wait = time from read to process start
		if !metrics.ReadTime.IsZero() && !metrics.ProcessStartTime.IsZero() {
			queueWait := metrics.ProcessStartTime.Sub(metrics.ReadTime)
			if queueWait > 0 {
				s.observerFor(s.queueWaitBy, s.queueWaitDuration, key).Observe(queueWait.Seconds())
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
