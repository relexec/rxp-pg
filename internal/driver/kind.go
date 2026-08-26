package driver

import (
	"context"
	"time"

	apicore "github.com/relexec/rxp/api/core"
	apierrors "github.com/relexec/rxp/api/errors"
	apikind "github.com/relexec/rxp/api/kind"
	apimetrics "github.com/relexec/rxp/api/metrics"
	apiquery "github.com/relexec/rxp/api/query"
	apisystem "github.com/relexec/rxp/api/system"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// KindRead reads a Kind from persistent storage.
func (d *Driver) KindRead(
	ctx context.Context,
	sel apikind.Selector,
) (*apikind.Kind, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeKind),
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

	err = d.kindReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	var sysRec *apisystem.System

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
	sel apikind.Selector,
) error {
	return sel.Validate()
}

// KindWrite atomically writes the supplied Kind to persistent storage.
func (d *Driver) KindWrite(
	ctx context.Context,
	k apikind.Kind,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeKind),
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

	err = d.kindWriteValidate(ctx, k)
	if err != nil {
		return err
	}

	var sysRec *apisystem.System

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
	k apikind.Kind,
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
	expr apiquery.Expression,
	opts ...apiquery.Option,
) (*apiquery.Result[*apikind.Kind], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeKind),
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

	qopts := apiquery.NewOptions(opts...)
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
	resOpts := apiquery.NewOptions(
		apiquery.Limit(boundedOpts.Limit()),
	)
	if len(recs) == int(boundedOpts.Limit()) {
		resOpts = apiquery.NewOptions(
			apiquery.ContinueFrom(recs[len(recs)-1].UUID),
			apiquery.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []apiquery.ResultModifier[*apikind.Kind]{
		apiquery.ResultWithItems(recs),
		apiquery.ResultWithOptions[*apikind.Kind](resOpts),
	}
	return apiquery.NewResult[*apikind.Kind](resNewOpts...), nil
}

// kindQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) kindQueryValidate(
	ctx context.Context,
	expr apiquery.Expression,
	opts apiquery.Options,
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
	opts apiquery.Options,
) apiquery.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultKindQueryLimit
	}
	limit = min(limit, MaxKindQueryLimit)
	return apiquery.NewOptions(apiquery.Limit(limit))
}
