package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/api/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RunEventsWrite persists a set of RunEvents to backend storage.
func (d *Driver) RunEventsWrite(
	ctx context.Context,
	run api.Run,
	events []api.RunEvent,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeRunEvent),
		}
		if err != nil {
			attrs = append(attrs, metrics.AttributeErrCode(err))
		}
		metrics.InstrumentWriteRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentWriteDuration.Record(ctx, elapsed)
	}()

	err = d.runEventsWriteValidate(ctx, run, events)
	if err != nil {
		return err
	}

	runRec, err := d.runStore.ReadByUUID(ctx, run.Request().UUID)
	if err != nil {
		return err
	}

	return d.runEventStore.Write(ctx, *runRec, events)
}

// runEventsWriteValidate returns an error if the supplied run and events are
// not valid.
func (d *Driver) runEventsWriteValidate(
	ctx context.Context,
	run api.Run,
	events []api.RunEvent,
) error {
	return run.Validate()
}
