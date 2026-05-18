package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/domain/write"
	writeoption "github.com/relexec/rxp/domain/write/option"
	"github.com/relexec/rxp/metrics"
	readoption "github.com/relexec/rxp/read/option"
	"github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// DomainRead reads a Domain from persistent storage.
func (d *Driver) DomainRead(
	ctx context.Context,
	sel types.Selector,
	opts ...readoption.Option,
) (types.Domain, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeTargetType(metrics.TargetTypeDomain),
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
	err = d.domainReadValidate(ctx, sel, ropts)
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
	sys := name.System()

	// Default the system to the host system if it hasn't been specified in the
	// selector.
	if sys == nil {
		sys = d.hostSystemRecord.System
	}

	rec, err := d.domainStore.ReadByName(
		ctx, sys, types.DomainName(name.Name()),
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
	sel types.Selector,
	opts readoption.Options,
) error {
	err := sel.Validate()
	if err != nil {
		return err
	}
	// If we're not looking up by UUID, verify that the name supplied in the
	// selector is a valid DomainName.
	if sel.UUID() != "" {
		return nil
	}
	n := sel.Name().Name()
	dn := types.DomainName(n)
	return dn.Validate()
}

// DomainWrite atomically writes the supplied Domain to persistent storage.
func (d *Driver) DomainWrite(
	ctx context.Context,
	dom types.Domain,
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
			metrics.AttributeTargetType(metrics.TargetTypeDomain),
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
	err = d.domainWriteValidate(ctx, dom, wopts)
	if err != nil {
		return err
	}

	sys := dom.System()
	// Default the system to the host system if it hasn't been specified.
	if sys == nil {
		dom = domain.New(
			domain.WithUUID(dom.UUID()),
			domain.WithName(dom.Name()),
			domain.WithSystem(d.hostSystemRecord.System),
		)
	}
	return d.domainStore.Write(ctx, dom)
}

// domainWriteValidate returns an error if the supplied domain and write
// options are not valid for writing a single Domain.
func (d *Driver) domainWriteValidate(
	ctx context.Context,
	domain types.Domain,
	opts writeoption.Options,
) error {
	return domain.Validate()
}

// var _ read.DomainReader = (*Driver)(nil)
var _ write.DomainWriter = (*Driver)(nil)
