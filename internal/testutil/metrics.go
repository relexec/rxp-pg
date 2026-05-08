package testutil

import (
	"context"

	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/types"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Metrics returns the Metrics handler for the test suite.
func Metrics(ctx context.Context) (types.Metrics, error) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
	)
	return metrics.New(
		ctx,
		metrics.WithMeterProvider(mp),
		metrics.WithReader(reader),
	)
}
