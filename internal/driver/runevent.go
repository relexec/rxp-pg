package driver

import (
	"context"
	"time"

	apicore "github.com/relexec/rxp/api/core"
	apimetrics "github.com/relexec/rxp/api/metrics"
	apirun "github.com/relexec/rxp/api/run"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RunEventsWrite persists a set of RunEvents to backend storage.
func (d *Driver) RunEventsWrite(
	ctx context.Context,
	run apirun.Run,
	events []apirun.Event,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeRunEvent),
		}
		if err != nil {
			attrs = append(attrs, apimetrics.AttributeErrCode(err))
		}
		apimetrics.InstrumentWriteRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		apimetrics.InstrumentWriteDuration.Record(ctx, elapsed)
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
	run apirun.Run,
	events []apirun.Event,
) error {
	return run.Validate()
}
