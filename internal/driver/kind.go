package driver

import (
	"context"
	"time"

	apicore "github.com/relexec/rxp/api/core"
	apierrors "github.com/relexec/rxp/api/errors"
	rxpkind "github.com/relexec/rxp/api/kind"
	rxpquery "github.com/relexec/rxp/api/query"
	rxpsystem "github.com/relexec/rxp/api/system"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/relexec/rxp-pg/metrics"
)

// KindRead reads a Kind from persistent storage.
func (d *Driver) KindRead(
	ctx context.Context,
	sel rxpkind.Selector,
) (*rxpkind.Kind, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(apicore.TypeKind),
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

	var sysRec *rxpsystem.System

	name := sel.Name()
	sys := sel.System()

	// Default the system to the host system if it hasn't been specified in the
	// selector.
	if sys != nil && sys.UUID != d.hostSystemUUID {
		sysRec, err = d.systemStore.ReadByUUID(ctx, sys.UUID)
		if err != nil {
			if err == apierrors.ErrNotFound {
				return nil, apierrors.ErrSystemUnknown
			}
			return nil, err
		}
	} else {
		sysRec = d.hostSystemRecord
	}

	return d.kindStore.ReadByName(ctx, sysRec, name)
}

// kindReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Kind.
func (d *Driver) kindReadValidate(
	ctx context.Context,
	sel rxpkind.Selector,
) error {
	return sel.Validate()
}

// KindWrite atomically writes the supplied Kind to persistent storage.
func (d *Driver) KindWrite(
	ctx context.Context,
	k rxpkind.Kind,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(apicore.TypeKind),
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

	var sysRec *rxpsystem.System

	sys := k.System

	// Default the system to the host system if it hasn't been specified.
	if sys != nil && sys.UUID != d.hostSystemUUID {
		sysRec, err = d.systemStore.ReadByUUID(ctx, sys.UUID)
		if err != nil {
			if err == apierrors.ErrNotFound {
				return apierrors.ErrSystemUnknown
			}
			return err
		}
	} else {
		sysRec = d.hostSystemRecord
		k.System = d.hostSystemRecord
	}
	return d.kindStore.Write(ctx, sysRec, k)
}

// kindWriteValidate returns an error if the supplied kind and write
// options are not valid for writing a single Kind.
func (d *Driver) kindWriteValidate(
	ctx context.Context,
	k rxpkind.Kind,
) error {
	return k.Validate()
}

const (
	DefaultKindQueryLimit = 10
	MaxKindQueryLimit     = 100
)

// KindQuery queries zero or more Kinds from persistent storage.
func (d *Driver) KindQuery(
	ctx context.Context,
	expr rxpquery.Expression,
	opts ...rxpquery.Option,
) (*rxpquery.Result[*rxpkind.Kind], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(apicore.TypeKind),
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
	resOpts := rxpquery.NewOptions(
		rxpquery.Limit(boundedOpts.Limit()),
	)
	if len(recs) == int(boundedOpts.Limit()) {
		resOpts = rxpquery.NewOptions(
			rxpquery.ContinueFrom(recs[len(recs)-1].UUID),
			rxpquery.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []rxpquery.ResultModifier[*rxpkind.Kind]{
		rxpquery.ResultWithItems(recs),
		rxpquery.ResultWithOptions[*rxpkind.Kind](resOpts),
	}
	return rxpquery.NewResult[*rxpkind.Kind](resNewOpts...), nil
}

// kindQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) kindQueryValidate(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) error {
	if expr == nil {
		return apierrors.ErrQueryExpressionRequired
	}
	return nil
}

// kindQueryBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records queryed is less
// than the max page result.
func (d *Driver) kindQueryBoundedOptions(
	ctx context.Context,
	opts rxpquery.Options,
) rxpquery.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultKindQueryLimit
	}
	limit = min(limit, MaxKindQueryLimit)
	return rxpquery.NewOptions(rxpquery.Limit(limit))
}
