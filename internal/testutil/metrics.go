package testutil

import (
	"context"

	apimetrics "github.com/relexec/rxp/api/metrics"
	otelmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Metrics returns the Metrics handler for the test suite.
func Metrics(ctx context.Context) (*apimetrics.Handler, error) {
	reader := otelmetric.NewManualReader()
	mp := otelmetric.NewMeterProvider(
		otelmetric.WithReader(reader),
	)
	return apimetrics.New(
		ctx,
		apimetrics.WithMeterProvider(mp),
		apimetrics.WithReader(reader),
	)
}
