// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-metrics-prometheus Authors
// SPDX-License-Identifier: Apache-2.0

package bandwidth

const (
	defaultNamespace = "llingr"
	defaultSubsystem = "bandwidth"
)

// BandwidthOptions holds construction-time configuration for a Sink.
// Fields are unexported; configure via the With* Option constructors.
type BandwidthOptions struct {
	serviceName string
	teamName    string
	namespace   string
	subsystem   string
}

// Option configures BandwidthOptions.
type Option func(*BandwidthOptions)

// WithServiceName sets a default service label value for bandwidth metrics.
// When the inbound BandwidthMetrics packet carries a non-nil Service, that
// value wins; this option provides a fallback when the per-packet Service
// is unset.
func WithServiceName(name string) Option {
	return func(o *BandwidthOptions) {
		o.serviceName = name
	}
}

// WithTeamName sets a default team label value for bandwidth metrics. When
// the inbound BandwidthMetrics packet carries a non-nil Service with a
// non-empty Team, that value wins; this option provides a fallback when
// the per-packet team is unset.
func WithTeamName(name string) Option {
	return func(o *BandwidthOptions) {
		o.teamName = name
	}
}

// WithNamespace overrides the Prometheus metric namespace. Defaults to "llingr".
func WithNamespace(namespace string) Option {
	return func(o *BandwidthOptions) {
		o.namespace = namespace
	}
}

// WithSubsystem overrides the Prometheus metric subsystem. Defaults to "bandwidth".
// Pass an empty string to omit the subsystem entirely.
func WithSubsystem(subsystem string) Option {
	return func(o *BandwidthOptions) {
		o.subsystem = subsystem
	}
}

// processOptions builds BandwidthOptions from defaults and user-supplied options.
func processOptions(opts ...Option) BandwidthOptions {
	o := BandwidthOptions{
		namespace: defaultNamespace,
		subsystem: defaultSubsystem,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
