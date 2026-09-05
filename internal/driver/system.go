package driver

import (
	"context"
	"time"

	apicore "github.com/relexec/rxp/api/core"
	apierrors "github.com/relexec/rxp/api/errors"
	rxpquery "github.com/relexec/rxp/api/query"
	rxpsystem "github.com/relexec/rxp/api/system"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/relexec/rxp-pg/metrics"
)

// SystemRead reads a System from persistent storage.
func (d *Driver) SystemRead(
	ctx context.Context,
	sel rxpsystem.Selector,
) (*rxpsystem.System, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(apicore.TypeSystem),
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

	return d.systemStore.ReadByUUID(ctx, uuid)
}

// systemReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single System.
func (d *Driver) systemReadValidate(
	ctx context.Context,
	sel rxpsystem.Selector,
) error {
	return sel.Validate()
}

// systemRecordFromSystem examines the supplied System and returns the
// associated Record from the system store, short-circuiting the return of the
// host system record when the supplied System is nil or the UUIDs match.
func (d *Driver) systemRecordFromSystem(
	ctx context.Context,
	sys *rxpsystem.System,
) (*rxpsystem.System, error) {
	if sys == nil || sys.UUID == d.hostSystemUUID {
		return d.hostSystemRecord, nil
	}
	sysRec, err := d.systemStore.ReadByUUID(ctx, sys.UUID)
	if err != nil {
		if err == apierrors.ErrNotFound {
			return nil, apierrors.ErrSystemUnknown
		}
		return nil, err
	}
	return sysRec, nil
}

// SystemWrite atomically writes the supplied System to persistent storage.
func (d *Driver) SystemWrite(
	ctx context.Context,
	sys rxpsystem.System,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(apicore.TypeSystem),
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
	sys rxpsystem.System,
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
	expr rxpquery.Expression,
	opts ...rxpquery.Option,
) (*rxpquery.Result[*rxpsystem.System], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(apicore.TypeSystem),
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

	qopts := rxpquery.NewOptions(opts...)
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
	resOpts := rxpquery.NewOptions(
		rxpquery.Limit(boundedOpts.Limit()),
	)
	if len(recs) == int(boundedOpts.Limit()) {
		resOpts = rxpquery.NewOptions(
			rxpquery.ContinueFrom(recs[len(recs)-1].UUID),
			rxpquery.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []rxpquery.ResultModifier[*rxpsystem.System]{
		rxpquery.ResultWithItems(recs),
		rxpquery.ResultWithOptions[*rxpsystem.System](resOpts),
	}
	return rxpquery.NewResult[*rxpsystem.System](resNewOpts...), nil
}

// systemQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) systemQueryValidate(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) error {
	if expr == nil {
		return apierrors.ErrQueryExpressionRequired
	}
	return nil
}

// systemQueryBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records queryed is less
// than the max page result.
func (d *Driver) systemQueryBoundedOptions(
	ctx context.Context,
	opts rxpquery.Options,
) rxpquery.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultSystemQueryLimit
	}
	limit = min(limit, MaxSystemQueryLimit)
	return rxpquery.NewOptions(rxpquery.Limit(limit))
}
