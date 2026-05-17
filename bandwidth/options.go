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
	applicationName string
	teamName        string
	namespace       string
	subsystem       string
}

// Option configures BandwidthOptions.
type Option func(*BandwidthOptions)

// WithApplicationName sets the application label value for all bandwidth
// metrics emitted by this sink. When empty, the label is present but blank.
func WithApplicationName(name string) Option {
	return func(o *BandwidthOptions) {
		o.applicationName = name
	}
}

// WithTeamName sets a default team label value for bandwidth metrics. When
// the inbound BandwidthMetrics packet carries a non-nil Team (as the demux
// bandwidth aggregator stamps it after WithTeam), that value wins; this
// option provides a fallback for cases where the aggregator hasn't been
// configured with a team.
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
