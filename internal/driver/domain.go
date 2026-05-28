package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"
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

const (
	DefaultDomainQueryLimit = 10
	MaxDomainQueryLimit     = 100
)

// DomainQuery queries zero or more Domains from persistent storage.
func (d *Driver) DomainQuery(
	ctx context.Context,
	expr expression.Expression,
	opts ...query.Option,
) (*query.Result[*domain.Domain], error) {
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
		metrics.InstrumentQueryRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentQueryDuration.Record(ctx, elapsed)
	}()

	qopts := query.NewOptions(opts...)
	err = d.domainQueryValidate(ctx, expr, qopts)
	if err != nil {
		return nil, err
	}

	boundedOpts := d.domainQueryBoundedOptions(ctx, qopts)

	recs, err := d.domainStore.Query(
		ctx, expr, boundedOpts,
	)
	if err != nil {
		return nil, err
	}
	out := make([]*domain.Domain, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.Domain)
	}
	resOpts := query.NewOptions(
		query.Limit(boundedOpts.Limit()),
	)
	if len(recs) == boundedOpts.Limit() {
		resOpts = query.NewOptions(
			query.ContinueFrom(recs[len(recs)-1].Domain.UUID()),
			query.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []query.ResultModifier[*domain.Domain]{
		query.ResultWithItems(out),
		query.ResultWithOptions[*domain.Domain](resOpts),
	}
	return query.NewResult[*domain.Domain](resNewOpts...), nil
}

// domainQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) domainQueryValidate(
	ctx context.Context,
	expr expression.Expression,
	opts query.Options,
) error {
	if expr == nil {
		return errors.ErrQueryExpressionRequired
	}
	return nil
}

// domainQueryBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records queryed is
// less than the max page result.
func (d *Driver) domainQueryBoundedOptions(
	ctx context.Context,
	opts query.Options,
) query.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultDomainQueryLimit
	}
	limit = min(limit, MaxDomainQueryLimit)
	return query.NewOptions(query.Limit(limit))
}
