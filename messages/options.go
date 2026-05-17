// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-metrics-prometheus Authors
// SPDX-License-Identifier: Apache-2.0

package messages

const (
	defaultNamespace = "llingr"
	defaultSubsystem = "engine"
)

// MessageMetricsOptions holds construction-time configuration for a Sink.
// Fields are unexported; configure via the With* Option constructors.
type MessageMetricsOptions struct {
	namespace string
	subsystem string
}

// Option configures MessageMetricsOptions.
type Option func(*MessageMetricsOptions)

// WithNamespace overrides the Prometheus metric namespace. Defaults to "llingr".
func WithNamespace(namespace string) Option {
	return func(o *MessageMetricsOptions) {
		o.namespace = namespace
	}
}

// WithSubsystem overrides the Prometheus metric subsystem. Defaults to "engine".
// Pass an empty string to omit the subsystem entirely.
func WithSubsystem(subsystem string) Option {
	return func(o *MessageMetricsOptions) {
		o.subsystem = subsystem
	}
}

// processOptions builds MessageMetricsOptions from defaults and user-supplied options.
func processOptions(opts ...Option) MessageMetricsOptions {
	o := MessageMetricsOptions{
		namespace: defaultNamespace,
		subsystem: defaultSubsystem,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
