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
	namespace string
	subsystem string
}

// Option configures BandwidthOptions.
type Option func(*BandwidthOptions)

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
