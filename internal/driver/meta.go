package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/meta"
	"github.com/relexec/rxp/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MetaRead reads a Meta from persistent storage.
func (d *Driver) MetaRead(
	ctx context.Context,
	sel meta.Selector,
) (*meta.Meta, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	var kv api.KindVersion

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeMeta),
			metrics.AttributeKindVersion(kv),
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

	err = d.metaReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	sys := sel.System()

	// Default the system to the host system if it hasn't been specified in the
	// selector.
	if sys == nil {
		sys = d.hostSystemRecord.System
	}
	kv = sel.KindVersion()

	rec, err := d.metaStore.ReadByKindVersion(ctx, sys, kv)
	if err != nil {
		return nil, err
	}
	return rec.Meta, nil
}

// metaReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Meta.
func (d *Driver) metaReadValidate(
	ctx context.Context,
	sel meta.Selector,
) error {
	return sel.Validate()
}

// MetaWrite atomically writes the supplied Meta to persistent storage.
func (d *Driver) MetaWrite(
	ctx context.Context,
	m *meta.Meta,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	kv := m.KindVersion()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeMeta),
			metrics.AttributeKindVersion(kv),
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

	err = d.metaWriteValidate(ctx, m)
	if err != nil {
		return err
	}

	system := m.System()
	if system == nil {
		m.SetSystem(d.hostSystemRecord.System)
	}
	return d.metaStore.Write(ctx, m)
}

// metaWriteValidate returns an error if the supplied meta and write
// options are not valid for writing a single Meta.
func (d *Driver) metaWriteValidate(
	ctx context.Context,
	meta *meta.Meta,
) error {
	if meta == nil {
		return errors.RequiredParameterNil(
			"meta",
			errors.WithWrap(errors.ErrInvalidWriteRequest),
		)
	}
	return meta.Validate()
}
