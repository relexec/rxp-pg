package metrics

import (
	"go.opentelemetry.io/otel/sdk/metric"
)

// Handler handles OTEL Meters for the rxp store
type Handler struct {
	// mp is the OTEL metric.MeterProvider
	mp *metric.MeterProvider
	// exporter is the OTEL sdkmetric.Exporter
	exporter metric.Exporter
	// reader is the OTEL sdkmetric.Reader
	reader metric.Reader
}

// MeterProvider returns the OTEL metric.MeterProvider
func (h Handler) MeterProvider() *metric.MeterProvider {
	return h.mp
}

// Exporter returns the OTEL sdkmetric.Exporter.
func (h Handler) Exporter() metric.Exporter {
	return h.exporter
}

// Reader returns the OTEL sdkmetric.Reader.
func (h Handler) Reader() metric.Reader {
	return h.reader
}
