package metrics

import (
	"go.opentelemetry.io/otel/sdk/metric"
)

var (
	// DefaultDurationBoundaries contains the default boundaries for duration
	// histograms, in seconds.
	DefaultDurationBoundaries = []float64{
		0.005, 0.01, 0.05, 0.1, 0.5, 1.0,
	}
)

var (
	ViewReadDuration = metric.NewView(
		metric.Instrument{
			Name: InstrumentNameReadDuration,
		},
		metric.Stream{
			Aggregation: metric.AggregationExplicitBucketHistogram{
				Boundaries: DefaultDurationBoundaries,
			},
		},
	)
	ViewWriteDuration = metric.NewView(
		metric.Instrument{
			Name: InstrumentNameReadDuration,
		},
		metric.Stream{
			Aggregation: metric.AggregationExplicitBucketHistogram{
				Boundaries: DefaultDurationBoundaries,
			},
		},
	)
	Views = []metric.View{
		ViewReadDuration,
		ViewWriteDuration,
	}
)
