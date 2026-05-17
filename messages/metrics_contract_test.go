// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-metrics-prometheus Authors
// SPDX-License-Identifier: Apache-2.0

package messages

import (
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

// TestMetricsContract_Golden locks down the full set of metric names, types,
// help text, label keys, and histogram bucket boundaries served over HTTP at
// /metrics. Drift (a renamed metric, a dropped label, a changed bucket) fails
// the test with a diff. To intentionally rebaseline after a deliberate change,
// run: UPDATE_GOLDEN=1 go test ./messages/
//
// Sample VALUES are stripped before comparison so the contract is independent
// of the test inputs' magnitudes; only the structural shape is asserted.
func TestMetricsContract_Golden(t *testing.T) {
	s := New()

	// populate every metric in one sink call: all trait counters, both gauges,
	// all three histograms (process duration, dead letter duration, queue wait)
	fixedReadTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ctx := nexus.SinkContext{
		TopicName:       "orders",
		ConsumerGroup:   "order-processor",
		ApplicationName: "test-app",
		Team:            &nexus.Team{Name: "platform"},
	}
	m := nexus.Metrics{
		Partition:               0,
		Traits:                  nexus.ProcessError | nexus.ProcessPanic | nexus.DeadLetter | nexus.Duplicate | nexus.UsedOverflow,
		QueueDepth:              42,
		Offset:                  12345,
		ProcessDuration:         50 * time.Millisecond,
		WriteDeadLetterDuration: 25 * time.Millisecond,
		ReadTime:                fixedReadTime,
		ProcessStartTime:        fixedReadTime.Add(10 * time.Millisecond),
	}
	if err := s.MetricsSink()(ctx, m); err != nil {
		t.Fatalf("sink returned error: %v", err)
	}

	mux := http.NewServeMux()
	s.RegisterHandler(mux, "/metrics")
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	got := stripMetricValues(string(body))

	goldenPath := filepath.Join("testdata", "metrics.golden")

	// explicit rebaseline path
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(goldenPath, []byte(got), 0644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("updated golden file: %s", goldenPath)
		return
	}

	want, err := os.ReadFile(goldenPath)
	if errors.Is(err, fs.ErrNotExist) {
		// first run: seed the golden so plain `go test` works out of the box.
		// the seeded file should be committed to lock the contract.
		if err := os.WriteFile(goldenPath, []byte(got), 0644); err != nil {
			t.Fatalf("seed golden: %v", err)
		}
		t.Logf("seeded golden file: %s - commit it to lock the metrics contract", goldenPath)
		return
	}
	if err != nil {
		t.Fatalf("read golden %s: %v", goldenPath, err)
	}
	if got != string(want) {
		t.Errorf("metrics contract drift detected.\nIf intentional, rebaseline with: UPDATE_GOLDEN=1 go test ./messages/\n\n--- want (golden) ---\n%s\n--- got (current) ---\n%s", string(want), got)
	}
}

// stripMetricValues removes the trailing value (and optional timestamp) from
// each sample line, preserving the metric name, labels, and all # HELP/# TYPE
// directives. The result is a structural representation suitable for golden
// comparison: independent of sample magnitudes but sensitive to any change in
// names, labels, types, help text, or histogram buckets.
func stripMetricValues(body string) string {
	lines := strings.Split(body, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if line == "" || strings.HasPrefix(line, "#") {
			out = append(out, line)
			continue
		}
		if i := strings.LastIndex(line, " "); i >= 0 {
			line = line[:i]
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}
