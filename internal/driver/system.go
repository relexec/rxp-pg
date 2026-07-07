package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/query"
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
	sys system.System,
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
	sys system.System,
) error {
	return sys.Validate()
}

const (
	DefaultSystemQueryLimit = 10
	MaxSystemQueryLimit     = 100
)

// SystemQuery queries zero or more Systems from persistent storage.
func (d *Driver) SystemQuery(
	ctx context.Context,
	expr query.Expression,
	opts ...query.Option,
) (*query.Result[*system.System], error) {
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
		metrics.InstrumentQueryRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentQueryDuration.Record(ctx, elapsed)
	}()

	qopts := query.NewOptions(opts...)
	err = d.systemQueryValidate(ctx, expr, qopts)
	if err != nil {
		return nil, err
	}

	boundedOpts := d.systemQueryBoundedOptions(ctx, qopts)

	recs, err := d.systemStore.Query(
		ctx, expr, boundedOpts,
	)
	if err != nil {
		return nil, err
	}
	out := make([]*system.System, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.System)
	}
	resOpts := query.NewOptions(
		query.Limit(boundedOpts.Limit()),
	)
	if len(recs) == int(boundedOpts.Limit()) {
		resOpts = query.NewOptions(
			query.ContinueFrom(recs[len(recs)-1].System.UUID()),
			query.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []query.ResultModifier[*system.System]{
		query.ResultWithItems(out),
		query.ResultWithOptions[*system.System](resOpts),
	}
	return query.NewResult[*system.System](resNewOpts...), nil
}

// systemQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) systemQueryValidate(
	ctx context.Context,
	expr query.Expression,
	opts query.Options,
) error {
	if expr == nil {
		return errors.ErrQueryExpressionRequired
	}
	return nil
}

// systemQueryBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records queryed is less
// than the max page result.
func (d *Driver) systemQueryBoundedOptions(
	ctx context.Context,
	opts query.Options,
) query.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultSystemQueryLimit
	}
	limit = min(limit, MaxSystemQueryLimit)
	return query.NewOptions(query.Limit(limit))
}
