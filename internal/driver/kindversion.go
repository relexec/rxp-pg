package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind/kindversion"
	"github.com/relexec/rxp/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// KindVersionRead reads a KindVersion from persistent storage.
func (d *Driver) KindVersionRead(
	ctx context.Context,
	sel kindversion.Selector,
) (*kindversion.KindVersion, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	var name api.KindVersionName

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeKindVersion),
			metrics.AttributeKindVersion(name),
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

	err = d.kindversionReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	sys := sel.System()

	// Default the system to the host system if it hasn't been specified in the
	// selector.
	if sys == nil {
		sys = d.hostSystemRecord.System
	}
	name = sel.Name()

	rec, err := d.kindversionStore.ReadByName(ctx, sys, name)
	if err != nil {
		return nil, err
	}
	return rec.KindVersion, nil
}

// kindversionReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single KindVersion.
func (d *Driver) kindversionReadValidate(
	ctx context.Context,
	sel kindversion.Selector,
) error {
	return sel.Validate()
}

// KindVersionWrite atomically writes the supplied KindVersion to persistent storage.
func (d *Driver) KindVersionWrite(
	ctx context.Context,
	kv *kindversion.KindVersion,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	name := kv.Name()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeKindVersion),
			metrics.AttributeKindVersion(name),
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

	err = d.kindversionWriteValidate(ctx, kv)
	if err != nil {
		return err
	}

	system := kv.System()
	if system == nil {
		kv.SetSystem(d.hostSystemRecord.System)
	}
	return d.kindversionStore.Write(ctx, kv)
}

// kindversionWriteValidate returns an error if the supplied kindversion and write
// options are not valid for writing a single KindVersion.
func (d *Driver) kindversionWriteValidate(
	ctx context.Context,
	kv *kindversion.KindVersion,
) error {
	if kv == nil {
		return errors.RequiredParameterNil(
			"kindversion",
			errors.WithWrap(errors.ErrInvalidWriteRequest),
		)
	}
	return kv.Validate()
}
