package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	readoption "github.com/relexec/rxp/read/option"
	"github.com/relexec/rxp/system/read"
	"github.com/relexec/rxp/system/write"
	writeoption "github.com/relexec/rxp/system/write/option"
	"github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SystemRead reads a System from persistent storage.
func (d *Driver) SystemRead(
	ctx context.Context,
	sel types.Selector,
	opts ...readoption.Option,
) (types.System, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeTargetType(metrics.TargetTypeSystem),
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

	ropts := readoption.New(opts...)
	err = d.systemReadValidate(ctx, sel, ropts)
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
	sel types.Selector,
	opts readoption.Options,
) error {
	// For System lookup, we require a UUID lookup.
	if sel.UUID() == "" {
		return errors.ErrSelectorUUIDRequired
	}
	return nil
}

// SystemWrite atomically writes the supplied System to persistent storage.
func (d *Driver) SystemWrite(
	ctx context.Context,
	system types.System,
	opts ...writeoption.Option,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeTargetType(metrics.TargetTypeSystem),
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

	wopts := writeoption.New(opts...)
	err = d.systemWriteValidate(ctx, system, wopts)
	if err != nil {
		return err
	}
	return d.systemStore.Write(ctx, system)
}

// systemWriteValidate returns an error if the supplied system and write
// options are not valid for writing a single System.
func (d *Driver) systemWriteValidate(
	ctx context.Context,
	system types.System,
	opts writeoption.Options,
) error {
	return system.Validate()
}

var _ read.SystemReader = (*Driver)(nil)
var _ write.SystemWriter = (*Driver)(nil)
