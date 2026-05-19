package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/kind"
	"github.com/relexec/rxp/kind/write"
	writeoption "github.com/relexec/rxp/kind/write/option"
	"github.com/relexec/rxp/metrics"
	readoption "github.com/relexec/rxp/read/option"
	"github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// KindRead reads a Kind from persistent storage.
func (d *Driver) KindRead(
	ctx context.Context,
	sel types.Selector,
	opts ...readoption.Option,
) (types.Kind, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeTargetType(metrics.TargetTypeKind),
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
	err = d.kindReadValidate(ctx, sel, ropts)
	if err != nil {
		return nil, err
	}

	name := sel.Name()
	sys := name.System()

	// Default the system to the host system if it hasn't been specified in the
	// selector.
	if sys == nil {
		sys = d.hostSystemRecord.System
	}

	rec, err := d.kindStore.ReadByName(
		ctx, sys, types.KindName(name.Name()),
	)
	if err != nil {
		return nil, err
	}
	return rec.Kind, nil
}

// kindReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Kind.
func (d *Driver) kindReadValidate(
	ctx context.Context,
	sel types.Selector,
	opts readoption.Options,
) error {
	err := sel.Validate()
	if err != nil {
		return err
	}
	n := sel.Name().Name()
	dn := types.KindName(n)
	return dn.Validate()
}

// KindWrite atomically writes the supplied Kind to persistent storage.
func (d *Driver) KindWrite(
	ctx context.Context,
	k types.Kind,
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
			metrics.AttributeTargetType(metrics.TargetTypeKind),
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
	err = d.kindWriteValidate(ctx, k, wopts)
	if err != nil {
		return err
	}

	sys := k.System()
	// Default the system to the host system if it hasn't been specified.
	if sys == nil {
		k = kind.New(
			kind.WithName(k.Name()),
			kind.WithSystem(d.hostSystemRecord.System),
		)
	}
	return d.kindStore.Write(ctx, k)
}

// kindWriteValidate returns an error if the supplied kind and write
// options are not valid for writing a single Kind.
func (d *Driver) kindWriteValidate(
	ctx context.Context,
	k types.Kind,
	opts writeoption.Options,
) error {
	return k.Validate()
}

// var _ read.KindReader = (*Driver)(nil)
var _ write.KindWriter = (*Driver)(nil)
