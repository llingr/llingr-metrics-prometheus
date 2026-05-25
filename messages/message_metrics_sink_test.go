// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-metrics-prometheus Authors
// SPDX-License-Identifier: Apache-2.0

package messages

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

var testCtx = nexus.SinkContext{
	TopicName:     "orders",
	ConsumerGroup: "order-processor",
	Service:       &nexus.Service{Name: "test-app"},
}

func testLabels(partition string) prometheus.Labels {
	return prometheus.Labels{
		"topic":          testCtx.TopicName,
		"consumer_group": testCtx.ConsumerGroup,
		"service":        testCtx.Service.Name,
		"team":           "",
		"partition":      partition,
	}
}

// --- New() and registration ---

func TestNew_RegistersAllMetrics(t *testing.T) {
	s := New()
	if _, err := s.Registry().Gather(); err != nil {
		t.Fatalf("gather error: %v", err)
	}

	// no samples yet, but sending one message populates all metric names
	sink := s.MetricsSink()
	_ = sink(testCtx, nexus.Metrics{Partition: 0})

	families, err := s.Registry().Gather()
	if err != nil {
		t.Fatalf("gather error: %v", err)
	}

	var names []string
	for _, f := range families {
		names = append(names, f.GetName())
	}
	joined := strings.Join(names, ",")

	expected := []string{
		"llingr_engine_processed_total",
		"llingr_engine_queue_depth",
		"llingr_engine_current_offset",
	}
	for _, e := range expected {
		if !strings.Contains(joined, e) {
			t.Errorf("missing metric %s in registry, got: %s", e, joined)
		}
	}
}

func TestRegistry_ReturnsDedicatedRegistry(t *testing.T) {
	s := New()
	if s.Registry() == nil {
		t.Fatal("Registry() should not return nil")
	}
	// should not be the default registry
	if s.Registry() == prometheus.DefaultRegisterer.(*prometheus.Registry) {
		t.Error("should use a dedicated registry, not the default")
	}
}

// --- Processed counter ---

func TestMetricsSink_AlwaysIncrementsProcessed(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 3})
	_ = sink(testCtx, nexus.Metrics{Partition: 3})
	_ = sink(testCtx, nexus.Metrics{Partition: 3})

	val := testutil.ToFloat64(s.messagesProcessed.With(testLabels("3")))
	if val != 3 {
		t.Errorf("processed count: got %v, want 3", val)
	}
}

func TestMetricsSink_ProcessedPerPartition(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 0})
	_ = sink(testCtx, nexus.Metrics{Partition: 0})
	_ = sink(testCtx, nexus.Metrics{Partition: 5})

	p0 := testutil.ToFloat64(s.messagesProcessed.With(testLabels("0")))
	p5 := testutil.ToFloat64(s.messagesProcessed.With(testLabels("5")))

	if p0 != 2 {
		t.Errorf("partition 0 processed: got %v, want 2", p0)
	}
	if p5 != 1 {
		t.Errorf("partition 5 processed: got %v, want 1", p5)
	}
}

// --- Trait counters ---

func TestMetricsSink_ProcessError(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 0, Traits: nexus.ProcessError})

	errored := testutil.ToFloat64(s.messagesErrored.With(testLabels("0")))
	if errored != 1 {
		t.Errorf("errored count: got %v, want 1", errored)
	}
	// processed should also increment
	processed := testutil.ToFloat64(s.messagesProcessed.With(testLabels("0")))
	if processed != 1 {
		t.Errorf("processed count: got %v, want 1 (always increments)", processed)
	}
}

func TestMetricsSink_ProcessPanic(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 0, Traits: nexus.ProcessPanic})

	val := testutil.ToFloat64(s.messagesPanicked.With(testLabels("0")))
	if val != 1 {
		t.Errorf("panicked count: got %v, want 1", val)
	}
}

func TestMetricsSink_DeadLetter(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 0, Traits: nexus.DeadLetter})

	val := testutil.ToFloat64(s.messagesDeadLetter.With(testLabels("0")))
	if val != 1 {
		t.Errorf("dead letter count: got %v, want 1", val)
	}
}

func TestMetricsSink_Duplicate(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 0, Traits: nexus.Duplicate})

	val := testutil.ToFloat64(s.messagesDuplicate.With(testLabels("0")))
	if val != 1 {
		t.Errorf("duplicate count: got %v, want 1", val)
	}
}

func TestMetricsSink_UsedOverflow(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 0, Traits: nexus.UsedOverflow})

	val := testutil.ToFloat64(s.messagesOverflow.With(testLabels("0")))
	if val != 1 {
		t.Errorf("overflow count: got %v, want 1", val)
	}
}

func TestMetricsSink_MultipleTraits(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	// message that errored AND was dead-lettered
	_ = sink(testCtx, nexus.Metrics{
		Partition: 0,
		Traits:    nexus.ProcessError | nexus.DeadLetter,
	})

	errored := testutil.ToFloat64(s.messagesErrored.With(testLabels("0")))
	dl := testutil.ToFloat64(s.messagesDeadLetter.With(testLabels("0")))
	processed := testutil.ToFloat64(s.messagesProcessed.With(testLabels("0")))

	if errored != 1 {
		t.Errorf("errored: got %v, want 1", errored)
	}
	if dl != 1 {
		t.Errorf("dead letter: got %v, want 1", dl)
	}
	if processed != 1 {
		t.Errorf("processed: got %v, want 1", processed)
	}
}

func TestMetricsSink_NoTraits(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 0})

	// none of the trait counters should be touched
	if testutil.CollectAndCount(s.messagesErrored) != 0 {
		t.Error("errored should not be touched with no traits")
	}
	if testutil.CollectAndCount(s.messagesPanicked) != 0 {
		t.Error("panicked should not be touched with no traits")
	}
	if testutil.CollectAndCount(s.messagesDeadLetter) != 0 {
		t.Error("dead letter should not be touched with no traits")
	}
	if testutil.CollectAndCount(s.messagesDuplicate) != 0 {
		t.Error("duplicate should not be touched with no traits")
	}
	if testutil.CollectAndCount(s.messagesOverflow) != 0 {
		t.Error("overflow should not be touched with no traits")
	}
}

// --- Gauges ---

func TestMetricsSink_QueueDepth(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 0, QueueDepth: 42})

	val := testutil.ToFloat64(s.queueDepth.With(testLabels("0")))
	if val != 42 {
		t.Errorf("queue depth: got %v, want 42", val)
	}

	// gauge should update (not accumulate)
	_ = sink(testCtx, nexus.Metrics{Partition: 0, QueueDepth: 10})

	val = testutil.ToFloat64(s.queueDepth.With(testLabels("0")))
	if val != 10 {
		t.Errorf("queue depth after update: got %v, want 10", val)
	}
}

func TestMetricsSink_CurrentOffset(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 7, Offset: 123456})

	val := testutil.ToFloat64(s.currentOffset.With(testLabels("7")))
	if val != 123456 {
		t.Errorf("current offset: got %v, want 123456", val)
	}
}

// --- Histograms ---

func TestMetricsSink_ProcessDuration(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{
		Partition:       0,
		ProcessDuration: 50 * time.Millisecond,
	})

	count := testutil.CollectAndCount(s.processDuration)
	if count == 0 {
		t.Error("process duration histogram should have samples")
	}
}

func TestMetricsSink_ProcessDuration_ZeroNotRecorded(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 0, ProcessDuration: 0})

	count := testutil.CollectAndCount(s.processDuration)
	if count != 0 {
		t.Errorf("process duration: got %d samples, want 0 for zero duration", count)
	}
}

func TestMetricsSink_DeadLetterDuration(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{
		Partition:               0,
		Traits:                  nexus.DeadLetter,
		WriteDeadLetterDuration: 25 * time.Millisecond,
	})

	count := testutil.CollectAndCount(s.deadLetterDuration)
	if count == 0 {
		t.Error("dead letter duration histogram should have samples")
	}
}

func TestMetricsSink_DeadLetterDuration_ZeroNotRecorded(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 0, WriteDeadLetterDuration: 0})

	count := testutil.CollectAndCount(s.deadLetterDuration)
	if count != 0 {
		t.Errorf("dead letter duration: got %d samples, want 0", count)
	}
}

func TestMetricsSink_QueueWaitDuration(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	now := time.Now()
	_ = sink(testCtx, nexus.Metrics{
		Partition:        0,
		ReadTime:         now,
		ProcessStartTime: now.Add(10 * time.Millisecond),
	})

	count := testutil.CollectAndCount(s.queueWaitDuration)
	if count == 0 {
		t.Error("queue wait duration histogram should have samples")
	}
}

func TestMetricsSink_QueueWaitDuration_ZeroTimesNotRecorded(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	// zero ReadTime
	_ = sink(testCtx, nexus.Metrics{
		Partition:        0,
		ProcessStartTime: time.Now(),
	})

	// zero ProcessStartTime
	_ = sink(testCtx, nexus.Metrics{
		Partition: 0,
		ReadTime:  time.Now(),
	})

	// both zero
	_ = sink(testCtx, nexus.Metrics{Partition: 0})

	count := testutil.CollectAndCount(s.queueWaitDuration)
	if count != 0 {
		t.Errorf("queue wait duration: got %d samples, want 0 for zero times", count)
	}
}

func TestMetricsSink_QueueWaitDuration_NegativeNotRecorded(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	now := time.Now()
	_ = sink(testCtx, nexus.Metrics{
		Partition:        0,
		ReadTime:         now,
		ProcessStartTime: now.Add(-10 * time.Millisecond), // before read
	})

	count := testutil.CollectAndCount(s.queueWaitDuration)
	if count != 0 {
		t.Errorf("queue wait duration: got %d samples, want 0 for negative wait", count)
	}
}

// --- Labels ---

func TestMetricsSink_LabelsFromSinkContext(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	ctx := nexus.SinkContext{
		TopicName:     "payments",
		ConsumerGroup: "payment-group",
		Service:       &nexus.Service{Name: "billing-service"},
	}

	_ = sink(ctx, nexus.Metrics{Partition: 11})

	labels := prometheus.Labels{
		"topic":          "payments",
		"consumer_group": "payment-group",
		"service":        "billing-service",
		"team":           "",
		"partition":      "11",
	}

	val := testutil.ToFloat64(s.messagesProcessed.With(labels))
	if val != 1 {
		t.Errorf("processed with custom labels: got %v, want 1", val)
	}
}

func TestMetricsSink_NoService(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	ctx := nexus.SinkContext{
		TopicName:     "events",
		ConsumerGroup: "cg1",
	}

	_ = sink(ctx, nexus.Metrics{Partition: 0})

	labels := prometheus.Labels{
		"topic":          "events",
		"consumer_group": "cg1",
		"service":        "",
		"team":           "",
		"partition":      "0",
	}

	val := testutil.ToFloat64(s.messagesProcessed.With(labels))
	if val != 1 {
		t.Errorf("processed with nil Service: got %v, want 1", val)
	}
}

// --- Accumulation ---

func TestMetricsSink_CountersAccumulate(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	for i := 0; i < 100; i++ {
		_ = sink(testCtx, nexus.Metrics{Partition: 0})
	}

	val := testutil.ToFloat64(s.messagesProcessed.With(testLabels("0")))
	if val != 100 {
		t.Errorf("accumulated processed: got %v, want 100", val)
	}
}

// --- Return value ---

func TestMetricsSink_ReturnsNil(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	err := sink(testCtx, nexus.Metrics{Partition: 0})
	if err != nil {
		t.Errorf("MetricsSink should always return nil, got %v", err)
	}
}

// --- Team label propagation ---
//
// All other tests in this file use testCtx with a Service that has only a
// Name (team empty). The tests below verify that when Service.Team is also
// populated, the team name flows through to the prometheus label.

var testCtxWithTeam = nexus.SinkContext{
	TopicName:     "orders",
	ConsumerGroup: "order-processor",
	Service:       &nexus.Service{Name: "test-app", Team: "platform-eng"},
}

func testLabelsWithTeam(partition string) prometheus.Labels {
	return prometheus.Labels{
		"topic":          testCtxWithTeam.TopicName,
		"consumer_group": testCtxWithTeam.ConsumerGroup,
		"service":        testCtxWithTeam.Service.Name,
		"team":           testCtxWithTeam.Service.Team,
		"partition":      partition,
	}
}

func TestMetricsSink_TeamLabelPropagated(t *testing.T) {
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtxWithTeam, nexus.Metrics{Partition: 0})
	_ = sink(testCtxWithTeam, nexus.Metrics{Partition: 0})

	val := testutil.ToFloat64(s.messagesProcessed.With(testLabelsWithTeam("0")))
	if val != 2 {
		t.Errorf("processed count with team label: got %v, want 2", val)
	}
}

func TestMetricsSink_TeamlessAndTeamedAreDistinctSeries(t *testing.T) {
	// A teamless context and a teamed context should produce separate
	// prometheus time series, not be merged into one. This is the
	// operational point of carrying team through to the label.
	s := New()
	sink := s.MetricsSink()

	_ = sink(testCtx, nexus.Metrics{Partition: 0})         // team=""
	_ = sink(testCtx, nexus.Metrics{Partition: 0})         // team=""
	_ = sink(testCtxWithTeam, nexus.Metrics{Partition: 0}) // team="platform-eng"

	teamless := testutil.ToFloat64(s.messagesProcessed.With(testLabels("0")))
	teamed := testutil.ToFloat64(s.messagesProcessed.With(testLabelsWithTeam("0")))

	if teamless != 2 {
		t.Errorf("teamless count: got %v, want 2", teamless)
	}
	if teamed != 1 {
		t.Errorf("teamed count: got %v, want 1", teamed)
	}
}

func TestMetricsSink_NilServiceSafe(t *testing.T) {
	// Defensive: explicitly nil Service field should be handled identically to
	// an unset field, producing empty service/team labels rather than panicking
	s := New()
	sink := s.MetricsSink()

	ctxWithExplicitNil := nexus.SinkContext{
		TopicName:     "orders",
		ConsumerGroup: "order-processor",
		Service:       nil,
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("nil Service caused panic: %v", r)
		}
	}()

	if err := sink(ctxWithExplicitNil, nexus.Metrics{Partition: 0}); err != nil {
		t.Errorf("MetricsSink with nil Service returned error: %v", err)
	}

	// resulting series has empty service and team labels
	labels := prometheus.Labels{
		"topic":          "orders",
		"consumer_group": "order-processor",
		"service":        "",
		"team":           "",
		"partition":      "0",
	}
	val := testutil.ToFloat64(s.messagesProcessed.With(labels))
	if val != 1 {
		t.Errorf("nil-Service count: got %v, want 1", val)
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
	_ = s.MetricsSink()(testCtx, nexus.Metrics{Partition: 0})

	got := gatheredNames(t, s)
	if !strings.Contains(got, "acme_engine_processed_total") {
		t.Errorf("expected acme_engine_processed_total, got: %s", got)
	}
	if strings.Contains(got, "llingr_engine_") {
		t.Errorf("namespace override leaked default 'llingr', got: %s", got)
	}
}

func TestWithSubsystem(t *testing.T) {
	s := New(WithSubsystem("custom"))
	_ = s.MetricsSink()(testCtx, nexus.Metrics{Partition: 0})

	got := gatheredNames(t, s)
	if !strings.Contains(got, "llingr_custom_processed_total") {
		t.Errorf("expected llingr_custom_processed_total, got: %s", got)
	}
	if strings.Contains(got, "llingr_engine_") {
		t.Errorf("subsystem override leaked default 'engine', got: %s", got)
	}
}

func TestWithSubsystem_Empty(t *testing.T) {
	s := New(WithSubsystem(""))
	_ = s.MetricsSink()(testCtx, nexus.Metrics{Partition: 0})

	got := gatheredNames(t, s)
	if !strings.Contains(got, "llingr_processed_total") {
		t.Errorf("expected llingr_processed_total (no subsystem), got: %s", got)
	}
}

func TestWithNamespaceAndSubsystem(t *testing.T) {
	s := New(WithNamespace("acme"), WithSubsystem("messagebus"))
	_ = s.MetricsSink()(testCtx, nexus.Metrics{Partition: 0})

	got := gatheredNames(t, s)
	if !strings.Contains(got, "acme_messagebus_processed_total") {
		t.Errorf("expected acme_messagebus_processed_total, got: %s", got)
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
	}{
		{
			name:           "default",
			opts:           nil,
			expectedMetric: "llingr_engine_processed_total",
		},
		{
			name:           "WithNamespace",
			opts:           []Option{WithNamespace("acme")},
			expectedMetric: "acme_engine_processed_total",
			forbidden:      "llingr_engine_",
		},
		{
			name:           "WithSubsystem",
			opts:           []Option{WithSubsystem("messages")},
			expectedMetric: "llingr_messages_processed_total",
			forbidden:      "llingr_engine_",
		},
		{
			name:           "WithSubsystem empty",
			opts:           []Option{WithSubsystem("")},
			expectedMetric: "llingr_processed_total",
			forbidden:      "llingr_engine_",
		},
		{
			name:           "WithNamespace and WithSubsystem",
			opts:           []Option{WithNamespace("acme"), WithSubsystem("messagebus")},
			expectedMetric: "acme_messagebus_processed_total",
			forbidden:      "llingr_engine_",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.opts...)
			_ = s.MetricsSink()(testCtx, nexus.Metrics{Partition: 0})

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
		})
	}
}

func TestRegisterHandler_CustomPath(t *testing.T) {
	s := New()
	mux := http.NewServeMux()
	s.RegisterHandler(mux, "/custom/path")

	server := httptest.NewServer(mux)
	defer server.Close()

	// unregistered path should 404
	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 on unregistered /metrics, got %d", resp.StatusCode)
	}

	// registered path should 200
	resp, err = http.Get(server.URL + "/custom/path")
	if err != nil {
		t.Fatalf("GET /custom/path: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 on /custom/path, got %d", resp.StatusCode)
	}
}
