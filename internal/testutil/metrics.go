package testutil

import (
	"context"

	"github.com/relexec/rxp/api/metrics"
	otelmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Metrics returns the Metrics handler for the test suite.
func Metrics(ctx context.Context) (*metrics.Handler, error) {
	reader := otelmetric.NewManualReader()
	mp := otelmetric.NewMeterProvider(
		otelmetric.WithReader(reader),
	)
	return metrics.New(
		ctx,
		metrics.WithMeterProvider(mp),
		metrics.WithReader(reader),
	)
}
