// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-metrics-prometheus Authors
// SPDX-License-Identifier: Apache-2.0

package bandwidth

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestBandwidthMetricsSink_RecordsBytesAndMessages(t *testing.T) {
	s := New()
	sink := s.BandwidthMetricsSink()

	metrics := nexus.BandwidthMetrics{
		Ts:                    time.Now(),
		StatsIntervalDuration: time.Minute,
		BandwidthMetricsID:    "test-uuid-1",
		TopicName:             "orders",
		ConsumerGroup:         "order-processor",
		Brokers: []nexus.BrokerInfo{
			{ID: "1", Host: "broker-1", Port: "9092", Rack: "eu-west-1a"},
			{ID: "2", Host: "broker-2", Port: "9092", Rack: "eu-west-1b"},
			{ID: "3", Host: "broker-3", Port: "9092", Rack: "eu-west-1c"},
		},
		Partitions: []nexus.PartitionBandwidth{
			{
				ID:                   0,
				ReceivedBytes:        1024000,
				TransmittedBytes:     512000,
				ReceivedMessageCount: 1000,
				CompressedBytes:      768000,
				UncompressedBytes:    1024000,
				Compression:          "snappy",
				Leader:               "broker-1",
			},
			{
				ID:                   1,
				ReceivedBytes:        2048000,
				TransmittedBytes:     1024000,
				ReceivedMessageCount: 2000,
				Leader:               "broker-2",
			},
		},
	}

	err := sink("orders", metrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// verify received bytes
	receivedBytes := testutil.ToFloat64(s.receivedBytes.With(prometheus.Labels{
		"topic": "orders", "consumer_group": "order-processor", "application": "", "team": "", "partition": "0",
	}))
	if receivedBytes != 1024000 {
		t.Errorf("received bytes partition 0: got %v, want 1024000", receivedBytes)
	}

	receivedBytesP1 := testutil.ToFloat64(s.receivedBytes.With(prometheus.Labels{
		"topic": "orders", "consumer_group": "order-processor", "application": "", "team": "", "partition": "1",
	}))
	if receivedBytesP1 != 2048000 {
		t.Errorf("received bytes partition 1: got %v, want 2048000", receivedBytesP1)
	}

	// verify message count
	msgCount := testutil.ToFloat64(s.receivedMessages.With(prometheus.Labels{
		"topic": "orders", "consumer_group": "order-processor", "application": "", "team": "", "partition": "0",
	}))
	if msgCount != 1000 {
		t.Errorf("received messages partition 0: got %v, want 1000", msgCount)
	}

	// verify compressed bytes with compression label
	compBytes := testutil.ToFloat64(s.compressedBytes.With(prometheus.Labels{
		"topic": "orders", "consumer_group": "order-processor", "application": "", "team": "", "partition": "0", "compression": "snappy",
	}))
	if compBytes != 768000 {
		t.Errorf("compressed bytes: got %v, want 768000", compBytes)
	}

	// verify topology gauges
	brokers := testutil.ToFloat64(s.brokerCount.With(prometheus.Labels{
		"topic": "orders", "consumer_group": "order-processor", "application": "", "team": "",
	}))
	if brokers != 3 {
		t.Errorf("broker count: got %v, want 3", brokers)
	}

	partitions := testutil.ToFloat64(s.partitionCount.With(prometheus.Labels{
		"topic": "orders", "consumer_group": "order-processor", "application": "", "team": "",
	}))
	if partitions != 2 {
		t.Errorf("partition count: got %v, want 2", partitions)
	}

	interval := testutil.ToFloat64(s.statsInterval.With(prometheus.Labels{
		"topic": "orders", "consumer_group": "order-processor", "application": "", "team": "",
	}))
	if interval != 60 {
		t.Errorf("stats interval: got %v, want 60", interval)
	}
}

func TestBandwidthMetricsSink_Accumulates(t *testing.T) {
	s := New()
	sink := s.BandwidthMetricsSink()

	packet := nexus.BandwidthMetrics{
		Ts:                    time.Now(),
		StatsIntervalDuration: time.Minute,
		ConsumerGroup:         "cg1",
		Partitions: []nexus.PartitionBandwidth{
			{ID: 0, ReceivedBytes: 1000, ReceivedMessageCount: 10},
		},
	}

	// send two packets - counters should accumulate
	_ = sink("topic1", packet)
	_ = sink("topic1", packet)

	total := testutil.ToFloat64(s.receivedBytes.With(prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "", "partition": "0",
	}))
	if total != 2000 {
		t.Errorf("accumulated received bytes: got %v, want 2000", total)
	}

	totalMsgs := testutil.ToFloat64(s.receivedMessages.With(prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "", "partition": "0",
	}))
	if totalMsgs != 20 {
		t.Errorf("accumulated messages: got %v, want 20", totalMsgs)
	}
}

func TestBandwidthMetricsSink_RecordsUncompressedBytes(t *testing.T) {
	s := New()
	sink := s.BandwidthMetricsSink()

	metrics := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Partitions: []nexus.PartitionBandwidth{
			{
				ID:                0,
				ReceivedBytes:     1024000,
				CompressedBytes:   768000,
				UncompressedBytes: 1024000,
				Compression:       "snappy",
			},
		},
	}

	err := sink("topic1", metrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	uncompBytes := testutil.ToFloat64(s.uncompressedBytes.With(prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "", "partition": "0", "compression": "snappy",
	}))
	if uncompBytes != 1024000 {
		t.Errorf("uncompressed bytes: got %v, want 1024000", uncompBytes)
	}
}

func TestBandwidthMetricsSink_ZeroUncompressedBytesNotRecorded(t *testing.T) {
	s := New()
	sink := s.BandwidthMetricsSink()

	packet := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Partitions: []nexus.PartitionBandwidth{
			{ID: 0, ReceivedBytes: 5000, UncompressedBytes: 0},
		},
	}

	_ = sink("topic1", packet)

	count := testutil.CollectAndCount(s.uncompressedBytes)
	if count != 0 {
		t.Errorf("uncompressed bytes metric count: got %v, want 0 (should not record zero values)", count)
	}
}

func TestBandwidthMetricsSink_UnknownCompressionLabel(t *testing.T) {
	// when a source supplies byte counts but no Compression string,
	// the sink should fall back to "unknown" rather than dropping the metric
	s := New()
	sink := s.BandwidthMetricsSink()

	packet := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Partitions: []nexus.PartitionBandwidth{
			{
				ID:                0,
				CompressedBytes:   500,
				UncompressedBytes: 1000,
				Compression:       "",
			},
		},
	}

	_ = sink("topic1", packet)

	compBytes := testutil.ToFloat64(s.compressedBytes.With(prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "", "partition": "0", "compression": "unknown",
	}))
	if compBytes != 500 {
		t.Errorf("compressed bytes with unknown compression: got %v, want 500", compBytes)
	}

	uncompBytes := testutil.ToFloat64(s.uncompressedBytes.With(prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "", "partition": "0", "compression": "unknown",
	}))
	if uncompBytes != 1000 {
		t.Errorf("uncompressed bytes with unknown compression: got %v, want 1000", uncompBytes)
	}
}

func TestBandwidthMetricsSink_ZeroCompressedBytesNotRecorded(t *testing.T) {
	s := New()
	sink := s.BandwidthMetricsSink()

	// partition with no compression data (e.g. confluent-kafka-go)
	packet := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Partitions: []nexus.PartitionBandwidth{
			{ID: 0, ReceivedBytes: 5000, CompressedBytes: 0},
		},
	}

	_ = sink("topic1", packet)

	// compressed bytes counter should not have been touched
	count := testutil.CollectAndCount(s.compressedBytes)
	if count != 0 {
		t.Errorf("compressed bytes metric count: got %v, want 0 (should not record zero values)", count)
	}
}

func TestBandwidthMetricsSink_RegisterCollectorsTo(t *testing.T) {
	s := New()
	externalReg := prometheus.NewRegistry()

	s.RegisterCollectorsTo(externalReg)

	// send a packet so metrics are populated
	sink := s.BandwidthMetricsSink()
	_ = sink("test-topic", nexus.BandwidthMetrics{
		ConsumerGroup: "test-group",
		Brokers: []nexus.BrokerInfo{
			{ID: "1", Host: "broker-1", Port: "9092", Rack: "az-a"},
		},
		Partitions: []nexus.PartitionBandwidth{
			{ID: 0, ReceivedBytes: 100, ReceivedMessageCount: 1},
		},
	})

	// gather from external registry - should contain bandwidth metrics
	families, err := externalReg.Gather()
	if err != nil {
		t.Fatalf("gather error: %v", err)
	}

	var names []string
	for _, f := range families {
		names = append(names, f.GetName())
	}
	joined := strings.Join(names, ",")

	if !strings.Contains(joined, "llingr_bandwidth_received_bytes_total") {
		t.Errorf("external registry missing llingr_bandwidth_received_bytes_total, got: %s", joined)
	}
	if !strings.Contains(joined, "llingr_bandwidth_received_messages_total") {
		t.Errorf("external registry missing llingr_bandwidth_received_messages_total, got: %s", joined)
	}
	if !strings.Contains(joined, "llingr_bandwidth_broker_info") {
		t.Errorf("external registry missing llingr_bandwidth_broker_info, got: %s", joined)
	}
}

func TestBandwidthMetricsSink_WithApplicationName(t *testing.T) {
	s := New(WithApplicationName("order-service"))
	sink := s.BandwidthMetricsSink()

	metrics := nexus.BandwidthMetrics{
		Ts:                    time.Now(),
		StatsIntervalDuration: time.Minute,
		ConsumerGroup:         "order-processor",
		Brokers: []nexus.BrokerInfo{
			{ID: "1", Host: "broker-1", Port: "9092", Rack: "eu-west-1a"},
		},
		Partitions: []nexus.PartitionBandwidth{
			{ID: 0, ReceivedBytes: 1024, ReceivedMessageCount: 10},
		},
	}

	err := sink("orders", metrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// verify application label is set on per-partition counter
	receivedBytes := testutil.ToFloat64(s.receivedBytes.With(prometheus.Labels{
		"topic": "orders", "consumer_group": "order-processor", "application": "order-service", "team": "", "partition": "0",
	}))
	if receivedBytes != 1024 {
		t.Errorf("received bytes with application label: got %v, want 1024", receivedBytes)
	}

	// verify application label is set on topology gauge
	brokerCount := testutil.ToFloat64(s.brokerCount.With(prometheus.Labels{
		"topic": "orders", "consumer_group": "order-processor", "application": "order-service", "team": "",
	}))
	if brokerCount != 1 {
		t.Errorf("broker count with application label: got %v, want 1", brokerCount)
	}

	// verify application label is set on broker_info gauge
	info := testutil.ToFloat64(s.brokerInfo.With(prometheus.Labels{
		"topic": "orders", "consumer_group": "order-processor", "application": "order-service", "team": "",
		"broker_id": "1", "broker_host": "broker-1", "broker_port": "9092", "broker_rack": "eu-west-1a",
	}))
	if info != 1.0 {
		t.Errorf("broker_info with application label: got %v, want 1.0", info)
	}
}

func TestBandwidthMetricsSink_BrokerInfo(t *testing.T) {
	s := New(WithApplicationName("my-app"))
	sink := s.BandwidthMetricsSink()

	metrics := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Brokers: []nexus.BrokerInfo{
			{ID: "1", Host: "kafka-0.internal", Port: "9092", Rack: "us-east-1a"},
			{ID: "2", Host: "kafka-1.internal", Port: "9093", Rack: "us-east-1b"},
			{ID: "3", Host: "kafka-2.internal", Port: "9094", Rack: ""},
		},
		Partitions: []nexus.PartitionBandwidth{
			{ID: 0, ReceivedBytes: 100},
		},
	}

	err := sink("events", metrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// verify all three broker info gauges are set to 1.0
	for _, tc := range []struct {
		id, host, port, rack string
	}{
		{"1", "kafka-0.internal", "9092", "us-east-1a"},
		{"2", "kafka-1.internal", "9093", "us-east-1b"},
		{"3", "kafka-2.internal", "9094", ""},
	} {
		val := testutil.ToFloat64(s.brokerInfo.With(prometheus.Labels{
			"topic":          "events",
			"consumer_group": "cg1",
			"application":    "my-app",
			"team":           "",
			"broker_id":      tc.id,
			"broker_host":    tc.host,
			"broker_port":    tc.port,
			"broker_rack":    tc.rack,
		}))
		if val != 1.0 {
			t.Errorf("broker_info for broker %s: got %v, want 1.0", tc.id, val)
		}
	}

	// verify total series count matches broker count
	count := testutil.CollectAndCount(s.brokerInfo)
	if count != 3 {
		t.Errorf("broker_info series count: got %v, want 3", count)
	}
}

func TestBandwidthMetricsSink_BrokerInfoUpdatesOnTopologyChange(t *testing.T) {
	s := New()
	sink := s.BandwidthMetricsSink()

	// initial topology: 2 brokers
	initial := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Brokers: []nexus.BrokerInfo{
			{ID: "1", Host: "broker-1", Port: "9092", Rack: "az-a"},
			{ID: "2", Host: "broker-2", Port: "9092", Rack: "az-b"},
		},
		Partitions: []nexus.PartitionBandwidth{
			{ID: 0, ReceivedBytes: 100},
		},
	}
	_ = sink("topic1", initial)

	// verify initial state
	count := testutil.CollectAndCount(s.brokerInfo)
	if count != 2 {
		t.Fatalf("initial broker_info series: got %v, want 2", count)
	}

	// topology change: broker-3 added, broker-2 changed host
	updated := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Brokers: []nexus.BrokerInfo{
			{ID: "1", Host: "broker-1", Port: "9092", Rack: "az-a"},
			{ID: "2", Host: "broker-2-new", Port: "9092", Rack: "az-b"},
			{ID: "3", Host: "broker-3", Port: "9092", Rack: "az-c"},
		},
		Partitions: []nexus.PartitionBandwidth{
			{ID: 0, ReceivedBytes: 200},
		},
	}
	_ = sink("topic1", updated)

	// new broker should be visible
	val := testutil.ToFloat64(s.brokerInfo.With(prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "",
		"broker_id": "3", "broker_host": "broker-3", "broker_port": "9092", "broker_rack": "az-c",
	}))
	if val != 1.0 {
		t.Errorf("new broker_info for broker 3: got %v, want 1.0", val)
	}

	// broker-2 with new host should also be visible
	val2 := testutil.ToFloat64(s.brokerInfo.With(prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "",
		"broker_id": "2", "broker_host": "broker-2-new", "broker_port": "9092", "broker_rack": "az-b",
	}))
	if val2 != 1.0 {
		t.Errorf("updated broker_info for broker 2: got %v, want 1.0", val2)
	}

	// total series should now be 4 (old broker-2 label set persists alongside new)
	count = testutil.CollectAndCount(s.brokerInfo)
	if count != 4 {
		t.Errorf("broker_info series after topology change: got %v, want 4", count)
	}
}

// --- Team label propagation ---
//
// All other tests in this file pass BandwidthMetrics without Team set,
// exercising the "team optional, not configured" path (team label = "").
// The tests below verify that when Team IS set on the inbound packet (which
// is how llingr-demux's bandwidth aggregator stamps it after WithTeam()),
// the team name flows through to the prometheus label.

func TestBandwidthMetricsSink_TeamLabelPropagated(t *testing.T) {
	s := New()
	sink := s.BandwidthMetricsSink()

	packet := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Team:          &nexus.Team{Name: "platform-eng"},
		Brokers: []nexus.BrokerInfo{
			{ID: "1", Host: "broker-1", Port: "9092", Rack: "az-a"},
		},
		Partitions: []nexus.PartitionBandwidth{
			{ID: 0, ReceivedBytes: 1000, ReceivedMessageCount: 5},
		},
	}

	_ = sink("topic1", packet)

	partitionLabels := prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "platform-eng", "partition": "0",
	}
	if v := testutil.ToFloat64(s.receivedBytes.With(partitionLabels)); v != 1000 {
		t.Errorf("received bytes with team label: got %v, want 1000", v)
	}

	topologyLabels := prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "platform-eng",
	}
	if v := testutil.ToFloat64(s.brokerCount.With(topologyLabels)); v != 1 {
		t.Errorf("broker count with team label: got %v, want 1", v)
	}

	brokerInfoLabels := prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "platform-eng",
		"broker_id": "1", "broker_host": "broker-1", "broker_port": "9092", "broker_rack": "az-a",
	}
	if v := testutil.ToFloat64(s.brokerInfo.With(brokerInfoLabels)); v != 1.0 {
		t.Errorf("broker_info with team label: got %v, want 1.0", v)
	}
}

func TestBandwidthMetricsSink_TeamlessAndTeamedAreDistinctSeries(t *testing.T) {
	// A teamless packet and a teamed packet should produce separate
	// prometheus time series, not be merged into one.
	s := New()
	sink := s.BandwidthMetricsSink()

	teamless := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Partitions:    []nexus.PartitionBandwidth{{ID: 0, ReceivedBytes: 100}},
	}
	teamed := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Team:          &nexus.Team{Name: "platform-eng"},
		Partitions:    []nexus.PartitionBandwidth{{ID: 0, ReceivedBytes: 100}},
	}

	_ = sink("topic1", teamless)
	_ = sink("topic1", teamless)
	_ = sink("topic1", teamed)

	teamlessVal := testutil.ToFloat64(s.receivedBytes.With(prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "", "partition": "0",
	}))
	teamedVal := testutil.ToFloat64(s.receivedBytes.With(prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "platform-eng", "partition": "0",
	}))

	if teamlessVal != 200 {
		t.Errorf("teamless received bytes: got %v, want 200", teamlessVal)
	}
	if teamedVal != 100 {
		t.Errorf("teamed received bytes: got %v, want 100", teamedVal)
	}
}

func TestBandwidthMetricsSink_WithTeamNameFallback(t *testing.T) {
	// When the inbound packet has no Team (e.g. aggregator wasn't configured
	// with demux.WithTeam), the sink-level WithTeamName option fills the gap.
	s := New(WithTeamName("api"))
	sink := s.BandwidthMetricsSink()

	packet := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Team:          nil,
		Partitions:    []nexus.PartitionBandwidth{{ID: 0, ReceivedBytes: 100}},
	}
	_ = sink("topic1", packet)

	val := testutil.ToFloat64(s.receivedBytes.With(prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "api", "partition": "0",
	}))
	if val != 100 {
		t.Errorf("WithTeamName fallback: got %v, want 100", val)
	}
}

func TestBandwidthMetricsSink_PerPacketTeamWinsOverOption(t *testing.T) {
	// When BOTH the packet's Team and the sink's WithTeamName are set, the
	// per-packet value wins (the aggregator's explicit team is authoritative).
	s := New(WithTeamName("fallback"))
	sink := s.BandwidthMetricsSink()

	packet := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Team:          &nexus.Team{Name: "platform"},
		Partitions:    []nexus.PartitionBandwidth{{ID: 0, ReceivedBytes: 100}},
	}
	_ = sink("topic1", packet)

	val := testutil.ToFloat64(s.receivedBytes.With(prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "platform", "partition": "0",
	}))
	if val != 100 {
		t.Errorf("per-packet team should win, got %v want 100", val)
	}
	// "fallback" label set should NOT exist
	if c := testutil.CollectAndCount(s.receivedBytes); c != 1 {
		t.Errorf("expected exactly 1 series, got %d (sink option shouldn't create a second)", c)
	}
}

func TestBandwidthMetricsSink_NilTeamSafe(t *testing.T) {
	// Defensive: explicitly nil Team should be handled identically to an
	// unset field, producing an empty team label rather than panicking.
	s := New()
	sink := s.BandwidthMetricsSink()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("nil Team caused panic: %v", r)
		}
	}()

	packet := nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Team:          nil,
		Partitions:    []nexus.PartitionBandwidth{{ID: 0, ReceivedBytes: 100}},
	}
	if err := sink("topic1", packet); err != nil {
		t.Errorf("nil Team returned error: %v", err)
	}

	val := testutil.ToFloat64(s.receivedBytes.With(prometheus.Labels{
		"topic": "topic1", "consumer_group": "cg1", "application": "", "team": "", "partition": "0",
	}))
	if val != 100 {
		t.Errorf("nil-Team count: got %v, want 100", val)
	}
}

// --- Options ---

func gatheredNames(t *testing.T, s *Sink) string {
	t.Helper()
	families, err := s.Registry().Gather()
	if err != nil {
		t.Fatalf("gather error: %v", err)
	}
	var names []string
	for _, f := range families {
		names = append(names, f.GetName())
	}
	return strings.Join(names, ",")
}

func TestWithNamespace(t *testing.T) {
	s := New(WithNamespace("acme"))
	_ = s.BandwidthMetricsSink()("topic1", nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Partitions:    []nexus.PartitionBandwidth{{ID: 0, ReceivedBytes: 100}},
	})

	got := gatheredNames(t, s)
	if !strings.Contains(got, "acme_bandwidth_received_bytes_total") {
		t.Errorf("expected acme_bandwidth_received_bytes_total, got: %s", got)
	}
	if strings.Contains(got, "llingr_bandwidth_") {
		t.Errorf("namespace override leaked default 'llingr', got: %s", got)
	}
}

func TestWithSubsystem(t *testing.T) {
	s := New(WithSubsystem("traffic"))
	_ = s.BandwidthMetricsSink()("topic1", nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Partitions:    []nexus.PartitionBandwidth{{ID: 0, ReceivedBytes: 100}},
	})

	got := gatheredNames(t, s)
	if !strings.Contains(got, "llingr_traffic_received_bytes_total") {
		t.Errorf("expected llingr_traffic_received_bytes_total, got: %s", got)
	}
	if strings.Contains(got, "llingr_bandwidth_") {
		t.Errorf("subsystem override leaked default 'bandwidth', got: %s", got)
	}
}

func TestWithSubsystem_Empty(t *testing.T) {
	s := New(WithSubsystem(""))
	_ = s.BandwidthMetricsSink()("topic1", nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Partitions:    []nexus.PartitionBandwidth{{ID: 0, ReceivedBytes: 100}},
	})

	got := gatheredNames(t, s)
	if !strings.Contains(got, "llingr_received_bytes_total") {
		t.Errorf("expected llingr_received_bytes_total (no subsystem), got: %s", got)
	}
}

func TestWithNamespaceAndSubsystem(t *testing.T) {
	s := New(WithNamespace("acme"), WithSubsystem("network"))
	_ = s.BandwidthMetricsSink()("topic1", nexus.BandwidthMetrics{
		ConsumerGroup: "cg1",
		Partitions:    []nexus.PartitionBandwidth{{ID: 0, ReceivedBytes: 100}},
	})

	got := gatheredNames(t, s)
	if !strings.Contains(got, "acme_network_received_bytes_total") {
		t.Errorf("expected acme_network_received_bytes_total, got: %s", got)
	}
}

// --- HTTP handler ---
//
// Tests use httptest.NewServer to spin up a real HTTP server on a random
// localhost port, then make real HTTP GETs over the wire. This exercises
// the full request path (mux routing, promhttp handler, content-type
// negotiation) rather than synthesising calls in-process.

func TestRegisterHandler_Variants(t *testing.T) {
	cases := []struct {
		name           string
		opts           []Option
		expectedMetric string
		forbidden      string // a substring that must NOT appear
		assertLabel    string // optional: a label=value pair that should appear
	}{
		{
			name:           "default",
			opts:           nil,
			expectedMetric: "llingr_bandwidth_received_bytes_total",
		},
		{
			name:           "WithNamespace",
			opts:           []Option{WithNamespace("acme")},
			expectedMetric: "acme_bandwidth_received_bytes_total",
			forbidden:      "llingr_bandwidth_",
		},
		{
			name:           "WithSubsystem",
			opts:           []Option{WithSubsystem("traffic")},
			expectedMetric: "llingr_traffic_received_bytes_total",
			forbidden:      "llingr_bandwidth_",
		},
		{
			name:           "WithSubsystem empty",
			opts:           []Option{WithSubsystem("")},
			expectedMetric: "llingr_received_bytes_total",
			forbidden:      "llingr_bandwidth_",
		},
		{
			name:           "WithNamespace and WithSubsystem",
			opts:           []Option{WithNamespace("acme"), WithSubsystem("network")},
			expectedMetric: "acme_network_received_bytes_total",
			forbidden:      "llingr_bandwidth_",
		},
		{
			name:           "WithApplicationName",
			opts:           []Option{WithApplicationName("order-svc")},
			expectedMetric: "llingr_bandwidth_received_bytes_total",
			assertLabel:    `application="order-svc"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.opts...)
			_ = s.BandwidthMetricsSink()("topic1", nexus.BandwidthMetrics{
				ConsumerGroup: "cg1",
				Partitions:    []nexus.PartitionBandwidth{{ID: 0, ReceivedBytes: 100}},
			})

			mux := http.NewServeMux()
			s.RegisterHandler(mux, "/metrics")

			server := httptest.NewServer(mux)
			defer server.Close()

			resp, err := http.Get(server.URL + "/metrics")
			if err != nil {
				t.Fatalf("GET failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("expected status 200, got %d", resp.StatusCode)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			bodyStr := string(body)

			if !strings.Contains(bodyStr, tc.expectedMetric) {
				t.Errorf("expected metric %q not in HTTP response", tc.expectedMetric)
			}
			if tc.forbidden != "" && strings.Contains(bodyStr, tc.forbidden) {
				t.Errorf("forbidden substring %q leaked into HTTP response", tc.forbidden)
			}
			if tc.assertLabel != "" && !strings.Contains(bodyStr, tc.assertLabel) {
				t.Errorf("expected label %q not in HTTP response", tc.assertLabel)
			}
		})
	}
}

func TestRegisterHandler_CustomPath(t *testing.T) {
	s := New()
	mux := http.NewServeMux()
	s.RegisterHandler(mux, "/metrics/bandwidth")

	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 on unregistered /metrics, got %d", resp.StatusCode)
	}

	resp, err = http.Get(server.URL + "/metrics/bandwidth")
	if err != nil {
		t.Fatalf("GET /metrics/bandwidth: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 on /metrics/bandwidth, got %d", resp.StatusCode)
	}
}

// --- Registry ---

func TestRegistry_ReturnsNonNil(t *testing.T) {
	s := New()
	if s.Registry() == nil {
		t.Fatal("Registry() should not return nil")
	}
}

func TestRegistry_DedicatedNotDefault(t *testing.T) {
	s := New()
	if s.Registry() == prometheus.DefaultRegisterer.(*prometheus.Registry) {
		t.Error("should use a dedicated registry, not the default")
	}
}
