package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// DomainRead reads a Domain from persistent storage.
func (d *Driver) DomainRead(
	ctx context.Context,
	sel domain.Selector,
) (*domain.Domain, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeDomain),
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

	err = d.domainReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	uuid := sel.UUID()
	if uuid != "" {
		rec, err := d.domainStore.ReadByUUID(ctx, uuid)
		if err != nil {
			return nil, err
		}
		return rec.Domain, nil
	}

	name := sel.Name()
	sys := sel.System()

	// Default the system to the host system if it hasn't been specified in the
	// selector.
	if sys == nil {
		sys = d.hostSystemRecord.System
	}

	rec, err := d.domainStore.ReadByName(
		ctx, sys, name,
	)
	if err != nil {
		return nil, err
	}
	return rec.Domain, nil
}

// domainReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Domain.
func (d *Driver) domainReadValidate(
	ctx context.Context,
	sel domain.Selector,
) error {
	return sel.Validate()
}

// DomainWrite atomically writes the supplied Domain to persistent storage.
func (d *Driver) DomainWrite(
	ctx context.Context,
	dom *domain.Domain,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeDomain),
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

	err = d.domainWriteValidate(ctx, dom)
	if err != nil {
		return err
	}

	sys := dom.System()
	// Default the system to the host system if it hasn't been specified.
	if sys == nil {
		dom.SetSystem(d.hostSystemRecord.System)
	}
	return d.domainStore.Write(ctx, dom)
}

// domainWriteValidate returns an error if the supplied domain and write
// options are not valid for writing a single Domain.
func (d *Driver) domainWriteValidate(
	ctx context.Context,
	dom *domain.Domain,
) error {
	if dom == nil {
		return errors.RequiredParameterNil(
			"domain",
			errors.WithWrap(errors.ErrInvalidWriteRequest),
		)
	}
	return dom.Validate()
}
