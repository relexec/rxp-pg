package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"
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

const (
	DefaultKindQueryLimit = 10
	MaxKindQueryLimit     = 100
)

// KindQuery queries zero or more Kinds from persistent storage.
func (d *Driver) KindQuery(
	ctx context.Context,
	expr expression.Expression,
	opts ...query.Option,
) (*query.Result[*kind.Kind], error) {
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
		metrics.InstrumentQueryRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentQueryDuration.Record(ctx, elapsed)
	}()

	qopts := query.NewOptions(opts...)
	err = d.kindQueryValidate(ctx, expr, qopts)
	if err != nil {
		return nil, err
	}

	boundedOpts := d.kindQueryBoundedOptions(ctx, qopts)

	recs, err := d.kindStore.Query(
		ctx, expr, boundedOpts,
	)
	if err != nil {
		return nil, err
	}
	out := make([]*kind.Kind, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.Kind)
	}
	resOpts := query.NewOptions(
		query.Limit(boundedOpts.Limit()),
	)
	if len(recs) == int(boundedOpts.Limit()) {
		resOpts = query.NewOptions(
			query.ContinueFrom(recs[len(recs)-1].Kind.UUID()),
			query.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []query.ResultModifier[*kind.Kind]{
		query.ResultWithItems(out),
		query.ResultWithOptions[*kind.Kind](resOpts),
	}
	return query.NewResult[*kind.Kind](resNewOpts...), nil
}

// kindQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) kindQueryValidate(
	ctx context.Context,
	expr expression.Expression,
	opts query.Options,
) error {
	if expr == nil {
		return errors.ErrQueryExpressionRequired
	}
	return nil
}

// kindQueryBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records queryed is less
// than the max page result.
func (d *Driver) kindQueryBoundedOptions(
	ctx context.Context,
	opts query.Options,
) query.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultKindQueryLimit
	}
	limit = min(limit, MaxKindQueryLimit)
	return query.NewOptions(query.Limit(limit))
}
