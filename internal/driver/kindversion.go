package driver

import (
	"context"
	"time"

	apicore "github.com/relexec/rxp/api/core"
	apierrors "github.com/relexec/rxp/api/errors"
	apikindversion "github.com/relexec/rxp/api/kindversion"
	apimetrics "github.com/relexec/rxp/api/metrics"
	apisystem "github.com/relexec/rxp/api/system"
	"github.com/relexec/rxp/query"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// KindVersionRead reads a KindVersion from persistent storage.
func (d *Driver) KindVersionRead(
	ctx context.Context,
	sel apikindversion.Selector,
) (*apikindversion.KindVersion, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	var name apikindversion.Name

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeKindVersion),
			apimetrics.AttributeKindVersion(name),
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

	err = d.kindversionReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	var sysRec *apisystem.System

	name = sel.Name()
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

	kindRec, err := d.kindStore.ReadByName(ctx, sysRec, name.Kind())
	if err != nil {
		if err != nil {
			if err == apierrors.ErrNotFound {
				return nil, apierrors.ErrKindUnknown
			}
			return nil, err
		}
	}

	return d.kindversionStore.ReadByName(ctx, sysRec, kindRec, name)
}

// kindversionReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single KindVersion.
func (d *Driver) kindversionReadValidate(
	ctx context.Context,
	sel apikindversion.Selector,
) error {
	return sel.Validate()
}

// KindVersionWrite atomically writes the supplied KindVersion to persistent storage.
func (d *Driver) KindVersionWrite(
	ctx context.Context,
	kv apikindversion.KindVersion,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	name := kv.Name()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeKindVersion),
			apimetrics.AttributeKindVersion(name),
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

	err = d.kindversionWriteValidate(ctx, kv)
	if err != nil {
		return err
	}

	var sysRec *apisystem.System

	kn := kv.Name()
	sys := kv.System

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
		kv.System = d.hostSystemRecord
	}

	kindRec, err := d.kindStore.ReadByName(ctx, sysRec, kn.Kind())
	if err != nil {
		if err != nil {
			if err == apierrors.ErrNotFound {
				return apierrors.ErrKindUnknown
			}
			return err
		}
	}
	return d.kindversionStore.Write(ctx, sysRec, kindRec, kv)
}

// kindversionWriteValidate returns an error if the supplied kindversion and write
// options are not valid for writing a single KindVersion.
func (d *Driver) kindversionWriteValidate(
	ctx context.Context,
	kv apikindversion.KindVersion,
) error {
	return kv.Validate()
}

const (
	DefaultKindVersionQueryLimit = 10
	MaxKindVersionQueryLimit     = 100
)

// KindVersionQuery queries zero or more KindVersions from persistent storage.
func (d *Driver) KindVersionQuery(
	ctx context.Context,
	expr query.Expression,
	opts ...query.Option,
) (*query.Result[*apikindversion.KindVersion], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeKindVersion),
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
	err = d.kindversionQueryValidate(ctx, expr, qopts)
	if err != nil {
		return nil, err
	}

	boundedOpts := d.kindversionQueryBoundedOptions(ctx, qopts)

	recs, err := d.kindversionStore.Query(
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
			query.ContinueFrom(string(recs[len(recs)-1].Name())),
			query.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []query.ResultModifier[*apikindversion.KindVersion]{
		query.ResultWithItems(recs),
		query.ResultWithOptions[*apikindversion.KindVersion](resOpts),
	}
	return query.NewResult[*apikindversion.KindVersion](resNewOpts...), nil
}

// kindversionQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) kindversionQueryValidate(
	ctx context.Context,
	expr query.Expression,
	opts query.Options,
) error {
	if expr == nil {
		return apierrors.ErrQueryExpressionRequired
	}
	return nil
}

// kindversionQueryBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records queryed is less
// than the max page result.
func (d *Driver) kindversionQueryBoundedOptions(
	ctx context.Context,
	opts query.Options,
) query.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultKindVersionQueryLimit
	}
	limit = min(limit, MaxKindVersionQueryLimit)
	return query.NewOptions(query.Limit(limit))
}
