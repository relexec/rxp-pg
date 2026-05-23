package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind"
	"github.com/relexec/rxp/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// KindRead reads a Kind from persistent storage.
func (d *Driver) KindRead(
	ctx context.Context,
	sel kind.Selector,
) (*kind.Kind, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeKind),
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

	err = d.kindReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	name := sel.Name()
	sys := sel.System()

	// Default the system to the host system if it hasn't been specified in the
	// selector.
	if sys == nil {
		sys = d.hostSystemRecord.System
	}

	rec, err := d.kindStore.ReadByName(ctx, sys, name)
	if err != nil {
		return nil, err
	}
	return rec.Kind, nil
}

// kindReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Kind.
func (d *Driver) kindReadValidate(
	ctx context.Context,
	sel kind.Selector,
) error {
	return sel.Validate()
}

// KindWrite atomically writes the supplied Kind to persistent storage.
func (d *Driver) KindWrite(
	ctx context.Context,
	k *kind.Kind,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeKind),
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

	err = d.kindWriteValidate(ctx, k)
	if err != nil {
		return err
	}

	sys := k.System()
	// Default the system to the host system if it hasn't been specified.
	if sys == nil {
		k.SetSystem(d.hostSystemRecord.System)
	}
	return d.kindStore.Write(ctx, k)
}

// kindWriteValidate returns an error if the supplied kind and write
// options are not valid for writing a single Kind.
func (d *Driver) kindWriteValidate(
	ctx context.Context,
	k *kind.Kind,
) error {
	if k == nil {
		return errors.RequiredParameterNil(
			"kind",
			errors.WithWrap(errors.ErrInvalidWriteRequest),
		)
	}
	return k.Validate()
}
