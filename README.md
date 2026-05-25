# llingr-metrics-prometheus

[![CI](https://github.com/llingr/llingr-metrics-prometheus/actions/workflows/ci.yml/badge.svg)](https://github.com/llingr/llingr-metrics-prometheus/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/llingr/llingr-metrics-prometheus.svg)](https://pkg.go.dev/github.com/llingr/llingr-metrics-prometheus)
[![Go Report Card](https://goreportcard.com/badge/github.com/llingr/llingr-metrics-prometheus)](https://goreportcard.com/report/github.com/llingr/llingr-metrics-prometheus)
[![Tag](https://img.shields.io/github/v/tag/llingr/llingr-metrics-prometheus)](https://github.com/llingr/llingr-metrics-prometheus/tags)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/llingr/llingr-metrics-prometheus)](go.mod)

Prometheus integration for [llingr-nexus](https://github.com/llingr/llingr-nexus)
bandwidth and message processing telemetry.

## Overview

This package provides two sinks that implement `nexus.MetricsSink` and
`nexus.BandwidthMetricsSink`:

- **`messages`** - per-message metrics (counters, gauges, histograms) emitted
  on each `nexus.MetricsSink` call from a consumer engine.
- **`bandwidth`** - per-partition bandwidth telemetry emitted
  periodically by adapters that implement `nexus.BandwidthPort`.

Each sink owns its own Prometheus registry by default; the host application
chooses where and how to expose metrics via `RegisterHandler` or by merging
into a shared registry.

## Installation

```bash
go get github.com/llingr/llingr-metrics-prometheus
```

## Quick start (per-message)

```go
import (
    "net/http"
    "github.com/llingr/llingr-metrics-prometheus/messages"
)

promSink := messages.New(
    // optional: override namespace/subsystem (defaults "llingr"/"engine")
    // messages.WithNamespace("acme"),
    // messages.WithSubsystem("messagebus"),
)

mux := http.NewServeMux()
promSink.RegisterHandler(mux, "/metrics")
go http.ListenAndServe(":19090", mux)

// pass promSink.MetricsSink() to whatever consumes nexus.MetricsSink in your
// application. Team identity for labels comes from nexus.SinkContext on every
// call, supplied by the consumer engine upstream.
```

## Quick start (bandwidth)

```go
import (
    "net/http"
    "github.com/llingr/llingr-metrics-prometheus/bandwidth"
)

bwSink := bandwidth.New(
    bandwidth.WithApplicationName("order-service"), // useful options
    bandwidth.WithTeamName("platform"),
    // optional: override namespace/subsystem (defaults "llingr"/"bandwidth")
    // bandwidth.WithNamespace("acme"),
    // bandwidth.WithSubsystem("traffic"),
)

mux := http.NewServeMux()
bwSink.RegisterHandler(mux, "/metrics/bandwidth")
go http.ListenAndServe(":19091", mux)

// pass bwSink.BandwidthMetricsSink() to whatever consumes
// nexus.BandwidthMetricsSink in your application.
```

## Shared registry (one /metrics endpoint)

```go
msgSink := messages.New()
bwSink := bandwidth.New()
bwSink.RegisterCollectorsTo(msgSink.Registry())

mux := http.NewServeMux()
msgSink.RegisterHandler(mux, "/metrics") // serves both
```

## Metrics

Per-message metrics (namespace `llingr_engine_*`):

| Metric                         | Type      | Description                                 |
|--------------------------------|-----------|---------------------------------------------|
| `processed_total`              | counter   | every message ProcessMessage was called for |
| `errored_total`                | counter   | ProcessMessage returned an error            |
| `panicked_total`               | counter   | ProcessMessage panicked                     |
| `dead_lettered_total`          | counter   | message sent to dead-letter handler         |
| `duplicate_total`              | counter   | duplicate detected by engine                |
| `used_overflow_total`          | counter   | overflow capacity used                      |
| `queue_depth`                  | gauge     | current buffer depth                        |
| `current_offset`               | gauge     | per-partition latest offset processed       |
| `process_duration_seconds`     | histogram | ProcessMessage latency                      |
| `dead_letter_duration_seconds` | histogram | WriteDeadLetter latency                     |
| `queue_wait_duration_seconds`  | histogram | time from read to process start             |

Labels: `topic`, `consumer_group`, `application`, `team`, `partition`.

Bandwidth metrics (namespace `llingr_bandwidth_*`):

| Metric                              | Type    | Description                                              |
|-------------------------------------|---------|----------------------------------------------------------|
| `received_bytes_total`              | counter | cumulative bytes received per partition                  |
| `transmitted_bytes_total`           | counter | cumulative bytes transmitted per partition               |
| `received_messages_total`           | counter | cumulative messages received per partition               |
| `compressed_bytes_total`            | counter | wire bytes; zero when compression visibility unavailable |
| `uncompressed_bytes_total`          | counter | decompressed bytes; zero when unavailable                |
| `broker_count`                      | gauge   | brokers in cluster at last collection                    |
| `partition_count`                   | gauge   | assigned partitions at last collection                   |
| `stats_interval_seconds`            | gauge   | configured collection cadence                            |
| `last_collection_timestamp_seconds` | gauge   | unix timestamp of last collection                        |
| `broker_info`                       | gauge   | broker topology (info metric, always 1.0)                |

Labels: `topic`, `consumer_group`, `application`, `team`, `partition`, plus
`compression` (compressed/uncompressed only) and broker labels on `broker_info`.

## Correlating bandwidth and message metrics

Both sinks share the same five label dimensions, so PromQL `on(...)` joins can be used to
align bandwidth and message rates. Example: bytes per processed message, per team:

```promql
rate(llingr_bandwidth_received_bytes_total[5m])
  / on(topic, consumer_group, application, team, partition)
    rate(llingr_engine_processed_total[5m])
```

## Design principles

- **Dedicated registry** - each sink owns its registry to avoid namespace
  collisions with host application metrics.
- **Zero-value safe** - sinks handle empty fields and zero counters without
  recording noise.

## Licence

Apache-2.0 - see [LICENSE](./LICENSE) and [COPYRIGHT](./COPYRIGHT).
Contributions are governed by [CONTRIBUTING.md](./CONTRIBUTING.md).
