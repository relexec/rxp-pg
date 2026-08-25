package driver

import (
	"context"
	"time"

	apicore "github.com/relexec/rxp/api/core"
	apimetrics "github.com/relexec/rxp/api/metrics"
	apisystem "github.com/relexec/rxp/api/system"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/query"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SystemRead reads a System from persistent storage.
func (d *Driver) SystemRead(
	ctx context.Context,
	sel apisystem.Selector,
) (*apisystem.System, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeSystem),
		}
		if err != nil {
			attrs = append(attrs, apimetrics.AttributeErrCode(err))
		}
		apimetrics.InstrumentReadRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		apimetrics.InstrumentReadDuration.Record(ctx, elapsed)
	}()

	err = d.systemReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	uuid := sel.UUID()

	return d.systemStore.ReadByUUID(ctx, uuid)
}

// systemReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single System.
func (d *Driver) systemReadValidate(
	ctx context.Context,
	sel apisystem.Selector,
) error {
	return sel.Validate()
}

// systemRecordFromSystem examines the supplied System and returns the
// associated Record from the system store, short-circuiting the return of the
// host system record when the supplied System is nil or the UUIDs match.
func (d *Driver) systemRecordFromSystem(
	ctx context.Context,
	sys *apisystem.System,
) (*apisystem.System, error) {
	if sys == nil || sys.UUID == d.hostSystemUUID {
		return d.hostSystemRecord, nil
	}
	sysRec, err := d.systemStore.ReadByUUID(ctx, sys.UUID)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrSystemUnknown
		}
		return nil, err
	}
	return sysRec, nil
}

// SystemWrite atomically writes the supplied System to persistent storage.
func (d *Driver) SystemWrite(
	ctx context.Context,
	sys apisystem.System,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeSystem),
		}
		if err != nil {
			attrs = append(attrs, apimetrics.AttributeErrCode(err))
		}
		apimetrics.InstrumentWriteRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		apimetrics.InstrumentWriteDuration.Record(ctx, elapsed)
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
	sys apisystem.System,
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
) (*query.Result[*apisystem.System], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeSystem),
		}
		if err != nil {
			attrs = append(attrs, apimetrics.AttributeErrCode(err))
		}
		apimetrics.InstrumentQueryRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		apimetrics.InstrumentQueryDuration.Record(ctx, elapsed)
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
	resOpts := query.NewOptions(
		query.Limit(boundedOpts.Limit()),
	)
	if len(recs) == int(boundedOpts.Limit()) {
		resOpts = query.NewOptions(
			query.ContinueFrom(recs[len(recs)-1].UUID),
			query.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []query.ResultModifier[*apisystem.System]{
		query.ResultWithItems(recs),
		query.ResultWithOptions[*apisystem.System](resOpts),
	}
	return query.NewResult[*apisystem.System](resNewOpts...), nil
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
