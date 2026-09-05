package metrics

import (
	"context"

	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/metric"
)

type WithOption func(*Handler)

// New returns a new metrics handler.
func New(
	ctx context.Context,
	opts ...WithOption,
) (*Handler, error) {
	h := &Handler{}
	for _, opt := range opts {
		opt(h)
	}
	if h.exporter == nil {
		exp, err := stdoutmetric.New(stdoutmetric.WithPrettyPrint())
		if err != nil {
			return nil, err
		}
		h.exporter = exp
	}
	if h.reader == nil {
		reader := metric.NewPeriodicReader(h.exporter)
		h.reader = reader
	}
	if h.mp == nil {
		mp := metric.NewMeterProvider(
			metric.WithView(Views...),
			metric.WithReader(h.reader),
		)
		h.mp = mp
	}
	if err := h.init(); err != nil {
		return nil, err
	}
	return h, nil
}

// WithMeterProvider sets the Handler handler's MeterProvider.
func WithMeterProvider(mp *metric.MeterProvider) WithOption {
	return func(m *Handler) {
		m.mp = mp
	}
}

// WithExporter sets the Handler handler's Exporter.
func WithExporter(exp metric.Exporter) WithOption {
	return func(m *Handler) {
		m.exporter = exp
	}
}

// WithReader sets the Handler handler's Reader.
func WithReader(reader metric.Reader) WithOption {
	return func(m *Handler) {
		m.reader = reader
	}
}
