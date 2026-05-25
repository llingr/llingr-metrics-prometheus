// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-metrics-prometheus Authors
// SPDX-License-Identifier: Apache-2.0

// Package bandwidth provides a Prometheus-compatible BandwidthMetricsSink
// implementation: per-partition byte counters, message counts, and
// compression visibility (when the source exposes it) as Prometheus metrics.
//
// IMPORTANT: counter values are applied with Add(), so sources must supply
// per-interval deltas, not cumulative totals. Cumulative values cause
// quadratic counter growth.
package bandwidth

import (
	"net/http"
	"strconv"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Sink collects llingr bandwidth telemetry and exposes it for Prometheus scraping.
type Sink struct {
	registry    *prometheus.Registry
	serviceName string // fallback service label when metrics.Service is nil
	teamName    string // fallback team label when metrics.Service is nil or has empty Team

	// per-partition counters
	receivedBytes      *prometheus.CounterVec
	transmittedBytes   *prometheus.CounterVec
	receivedMessages   *prometheus.CounterVec
	compressedBytes    *prometheus.CounterVec
	uncompressedBytes  *prometheus.CounterVec

	// gauges
	brokerCount      *prometheus.GaugeVec
	partitionCount   *prometheus.GaugeVec
	statsInterval    *prometheus.GaugeVec
	lastCollectionTs *prometheus.GaugeVec
	brokerInfo       *prometheus.GaugeVec
}

// New creates a new Prometheus bandwidth metrics sink with its own registry.
// Use RegisterHandler() to expose metrics at a path chosen by the host application,
// or Registry() to merge with an existing registry. See options.go for available
// Option constructors.
func New(opts ...Option) *Sink {
	o := processOptions(opts...)
	s := &Sink{
		registry:    prometheus.NewRegistry(),
		serviceName: o.serviceName,
		teamName:    o.teamName,
	}

	partitionLabels := []string{"topic", "consumer_group", "service", "team", "partition"}
	compressionLabels := []string{"topic", "consumer_group", "service", "team", "partition", "compression"}
	topologyLabels := []string{"topic", "consumer_group", "service", "team"}
	brokerInfoLabels := []string{"topic", "consumer_group", "service", "team", "broker_id", "broker_host", "broker_port", "broker_rack"}

	s.receivedBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "received_bytes_total",
			Help:      "Total bytes received by llingr consumer instances",
		},
		partitionLabels,
	)

	s.transmittedBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "transmitted_bytes_total",
			Help:      "Total bytes transmitted by llingr consumer instances",
		},
		partitionLabels,
	)

	s.receivedMessages = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "received_messages_total",
			Help:      "Total messages received by llingr consumer instances",
		},
		partitionLabels,
	)

	s.compressedBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "compressed_bytes_total",
			Help:      "Total compressed (wire) bytes received; zero when compression visibility is unavailable",
		},
		compressionLabels,
	)

	s.uncompressedBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "uncompressed_bytes_total",
			Help:      "Total uncompressed (decompressed) bytes received; zero when compression visibility is unavailable",
		},
		compressionLabels,
	)

	s.brokerCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "broker_count",
			Help:      "Number of brokers in the cluster at last collection",
		},
		topologyLabels,
	)

	s.partitionCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "partition_count",
			Help:      "Number of assigned partitions at last collection",
		},
		topologyLabels,
	)

	s.statsInterval = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "stats_interval_seconds",
			Help:      "Configured collection cadence in seconds",
		},
		topologyLabels,
	)

	s.lastCollectionTs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "last_collection_timestamp_seconds",
			Help:      "Unix timestamp of the most recent bandwidth collection",
		},
		topologyLabels,
	)

	s.brokerInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: o.namespace,
			Subsystem: o.subsystem,
			Name:      "broker_info",
			Help:      "Broker topology at last collection (info metric, always 1.0)",
		},
		brokerInfoLabels,
	)

	s.registry.MustRegister(
		s.receivedBytes,
		s.transmittedBytes,
		s.receivedMessages,
		s.compressedBytes,
		s.uncompressedBytes,
		s.brokerCount,
		s.partitionCount,
		s.statsInterval,
		s.lastCollectionTs,
		s.brokerInfo,
	)

	return s
}

// serviceLabels resolves the service and team labels from a per-packet
// Service (winning when set) and the sink-level fallbacks. Empty strings
// are valid Prometheus label values and let dashboards filter untagged
// consumers cleanly
func (s *Sink) serviceLabels(service *nexus.Service) (svc, team string) {
	if service != nil {
		svc = service.Name
		team = service.Team
	}
	if svc == "" {
		svc = s.serviceName
	}
	if team == "" {
		team = s.teamName
	}
	return svc, team
}

// BandwidthMetricsSink returns a nexus.BandwidthMetricsSink function that
// records bandwidth telemetry to Prometheus counters and gauges.
//
// Counters are monotonically increasing - each BandwidthMetrics packet adds
// to the cumulative total. Prometheus rate() and increase() functions derive
// per-second and per-interval rates from these counters.
func (s *Sink) BandwidthMetricsSink() nexus.BandwidthMetricsSink {
	return func(topicName string, metrics nexus.BandwidthMetrics) error {
		group := metrics.ConsumerGroup
		// per-packet Service (set by the demux bandwidth aggregator) wins;
		// fall back to the sink-level WithServiceName / WithTeamName options
		// when the per-packet value is unset
		svc, team := s.serviceLabels(metrics.Service)

		topologyLabels := prometheus.Labels{
			"topic":          topicName,
			"consumer_group": group,
			"service":        svc,
			"team":           team,
		}

		// topology gauges
		s.brokerCount.With(topologyLabels).Set(float64(len(metrics.Brokers)))
		s.partitionCount.With(topologyLabels).Set(float64(len(metrics.Partitions)))
		s.statsInterval.With(topologyLabels).Set(metrics.StatsIntervalDuration.Seconds())

		if !metrics.Ts.IsZero() {
			s.lastCollectionTs.With(topologyLabels).Set(float64(metrics.Ts.Unix()))
		}

		// broker info gauges - one series per broker node
		for _, b := range metrics.Brokers {
			s.brokerInfo.With(prometheus.Labels{
				"topic":          topicName,
				"consumer_group": group,
				"service":        svc,
				"team":           team,
				"broker_id":      b.ID,
				"broker_host":    b.Host,
				"broker_port":    b.Port,
				"broker_rack":    b.Rack,
			}).Set(1.0)
		}

		// per-partition counters
		for _, p := range metrics.Partitions {
			partition := strconv.FormatInt(int64(p.ID), 10)
			labels := prometheus.Labels{
				"topic":          topicName,
				"consumer_group": group,
				"service":        svc,
				"team":           team,
				"partition":      partition,
			}

			if p.ReceivedBytes > 0 {
				s.receivedBytes.With(labels).Add(float64(p.ReceivedBytes))
			}
			if p.TransmittedBytes > 0 {
				s.transmittedBytes.With(labels).Add(float64(p.TransmittedBytes))
			}
			if p.ReceivedMessageCount > 0 {
				s.receivedMessages.With(labels).Add(float64(p.ReceivedMessageCount))
			}
			if p.CompressedBytes > 0 {
				compression := p.Compression
				if compression == "" {
					compression = "unknown"
				}
				compLabels := prometheus.Labels{
					"topic":          topicName,
					"consumer_group": group,
					"service":        svc,
					"team":           team,
					"partition":      partition,
					"compression":    compression,
				}
				s.compressedBytes.With(compLabels).Add(float64(p.CompressedBytes))
			}
			if p.UncompressedBytes > 0 {
				compression := p.Compression
				if compression == "" {
					compression = "unknown"
				}
				s.uncompressedBytes.With(prometheus.Labels{
					"topic":          topicName,
					"consumer_group": group,
					"service":        svc,
					"team":           team,
					"partition":      partition,
					"compression":    compression,
				}).Add(float64(p.UncompressedBytes))
			}
		}

		return nil
	}
}

// RegisterHandler registers the Prometheus metrics handler at the specified path
// on the provided ServeMux. This allows bandwidth metrics to be served on a
// different path or port from the per-message metrics if desired.
//
// Example:
//
//	bwSink := bandwidth.New()
//	mux := http.NewServeMux()
//	bwSink.RegisterHandler(mux, "/metrics/bandwidth")
//	http.ListenAndServe(":8080", mux)
func (s *Sink) RegisterHandler(mux *http.ServeMux, path string) {
	handler := promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
		Timeout:           10 * time.Second,
	})
	mux.Handle(path, handler)
}

// Registry returns the underlying Prometheus registry. Use this to merge
// bandwidth metrics into an existing registry or to combine with the
// per-message sink's registry.
//
// Example (shared registry):
//
//	msgSink := messages.New()
//	bwSink := bandwidth.New()
//	// Register bwSink collectors into msgSink's registry:
//	bwSink.RegisterCollectorsTo(msgSink.Registry())
func (s *Sink) Registry() *prometheus.Registry {
	return s.registry
}

// RegisterCollectorsTo registers all bandwidth metric collectors into an
// external registry. This allows both per-message and bandwidth metrics to
// be served from a single /metrics endpoint.
//
// Example:
//
//	msgSink := messages.New()
//	bwSink := bandwidth.New()
//	bwSink.RegisterCollectorsTo(msgSink.Registry())
//	msgSink.RegisterHandler(mux, "/metrics") // serves both
func (s *Sink) RegisterCollectorsTo(reg *prometheus.Registry) {
	reg.MustRegister(
		s.receivedBytes,
		s.transmittedBytes,
		s.receivedMessages,
		s.compressedBytes,
		s.uncompressedBytes,
		s.brokerCount,
		s.partitionCount,
		s.statsInterval,
		s.lastCollectionTs,
		s.brokerInfo,
	)
}
