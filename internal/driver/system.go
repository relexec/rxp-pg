package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/system"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SystemRead reads a System from persistent storage.
func (d *Driver) SystemRead(
	ctx context.Context,
	sel system.Selector,
) (*system.System, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeSystem),
		}
		if err != nil {
			attrs = append(attrs, metrics.AttributeErrCode(err))
		}
		metrics.InstrumentReadRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentReadDuration.Record(ctx, elapsed)
	}()

	err = d.systemReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	uuid := sel.UUID()

	rec, err := d.systemStore.ReadByUUID(ctx, uuid)
	if err != nil {
		return nil, err
	}
	return rec.System, nil
}

// systemReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single System.
func (d *Driver) systemReadValidate(
	ctx context.Context,
	sel system.Selector,
) error {
	return sel.Validate()
}

// SystemWrite atomically writes the supplied System to persistent storage.
func (d *Driver) SystemWrite(
	ctx context.Context,
	sys *system.System,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeSystem),
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

	err = d.systemWriteValidate(ctx, sys)
	if err != nil {
		return err
	}
	return d.systemStore.Write(ctx, sys)
}

// systemWriteValidate returns an error if the supplied system and write
// options are not valid for writing a single System.
func (d *Driver) systemWriteValidate(
	ctx context.Context,
	sys *system.System,
) error {
	if sys == nil {
		return errors.RequiredParameterNil(
			"system",
			errors.WithWrap(errors.ErrInvalidWriteRequest),
		)
	}
	return sys.Validate()
}
