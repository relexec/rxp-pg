package metrics

import (
	"go.opentelemetry.io/otel/metric"
)

const (
	ProviderName = "rxp-pg"

	InstrumentNameReadRequest = "read.request"
	InstrumentDescReadRequest = "Number of read operations. Labels: 'error.code', 'type'."

	InstrumentNameReadDuration = "read.duration"
	InstrumentDescReadDuration = "Histogram of read operation duration in seconds. Labels: 'error.code', 'type'."

	InstrumentNameWriteRequest = "write.request"
	InstrumentDescWriteRequest = "Number of write operations. Labels: 'error.code', 'type'."

	InstrumentNameWriteDuration = "write.duration"
	InstrumentDescWriteDuration = "Histogram of write operation duration in seconds. Labels: 'error.code', 'type'."

	InstrumentNameQueryRequest = "query.request"
	InstrumentDescQueryRequest = "Number of query operations. Labels: 'error.code', 'type'."

	InstrumentNameQueryDuration = "query.duration"
	InstrumentDescQueryDuration = "Histogram of query operation duration in seconds. Labels: 'error.code', 'type'."
)

var (
	InstrumentReadRequest   metric.Int64Counter
	InstrumentReadDuration  metric.Float64Histogram
	InstrumentWriteRequest  metric.Int64Counter
	InstrumentWriteDuration metric.Float64Histogram
	InstrumentQueryRequest  metric.Int64Counter
	InstrumentQueryDuration metric.Float64Histogram
)

// init initializes the rxp metrics handler.
func (h *Handler) init() error {
	var err error
	p := h.MeterProvider()
	m := p.Meter(ProviderName)

	InstrumentReadRequest, err = m.Int64Counter(
		InstrumentNameReadRequest,
		metric.WithDescription(InstrumentDescReadRequest),
		metric.WithUnit("{call}"),
	)
	if err != nil {
		return err
	}

	InstrumentWriteRequest, err = m.Int64Counter(
		InstrumentNameWriteRequest,
		metric.WithDescription(InstrumentDescWriteRequest),
		metric.WithUnit("{call}"),
	)
	if err != nil {
		return err
	}

	InstrumentReadDuration, err = m.Float64Histogram(
		InstrumentNameReadDuration,
		metric.WithDescription(InstrumentDescReadDuration),
		metric.WithUnit("{seconds}"),
	)
	if err != nil {
		return err
	}

	InstrumentWriteDuration, err = m.Float64Histogram(
		InstrumentNameWriteDuration,
		metric.WithDescription(InstrumentDescWriteDuration),
		metric.WithUnit("{seconds"),
	)
	if err != nil {
		return err
	}

	InstrumentQueryRequest, err = m.Int64Counter(
		InstrumentNameQueryRequest,
		metric.WithDescription(InstrumentDescQueryRequest),
		metric.WithUnit("{call}"),
	)
	if err != nil {
		return err
	}

	InstrumentQueryDuration, err = m.Float64Histogram(
		InstrumentNameQueryDuration,
		metric.WithDescription(InstrumentDescQueryDuration),
		metric.WithUnit("{seconds}"),
	)
	if err != nil {
		return err
	}
	return nil
}
