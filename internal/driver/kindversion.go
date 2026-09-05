package driver

import (
	"context"
	"time"

	apicore "github.com/relexec/rxp/api/core"
	apierrors "github.com/relexec/rxp/api/errors"
	rxpkindversion "github.com/relexec/rxp/api/kindversion"
	rxpquery "github.com/relexec/rxp/api/query"
	rxpsystem "github.com/relexec/rxp/api/system"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/relexec/rxp-pg/metrics"
)

// KindVersionRead reads a KindVersion from persistent storage.
func (d *Driver) KindVersionRead(
	ctx context.Context,
	sel rxpkindversion.Selector,
) (*rxpkindversion.KindVersion, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	var name rxpkindversion.Name

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(apicore.TypeKindVersion),
			metrics.AttributeKindVersion(name),
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

	err = d.kindversionReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	var sysRec *rxpsystem.System

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
	sel rxpkindversion.Selector,
) error {
	return sel.Validate()
}

// KindVersionWrite atomically writes the supplied KindVersion to persistent storage.
func (d *Driver) KindVersionWrite(
	ctx context.Context,
	kv rxpkindversion.KindVersion,
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
			metrics.AttributeType(apicore.TypeKindVersion),
			metrics.AttributeKindVersion(name),
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

	err = d.kindversionWriteValidate(ctx, kv)
	if err != nil {
		return err
	}

	var sysRec *rxpsystem.System

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
	kv rxpkindversion.KindVersion,
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
	expr rxpquery.Expression,
	opts ...rxpquery.Option,
) (*rxpquery.Result[*rxpkindversion.KindVersion], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(apicore.TypeKindVersion),
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
	resOpts := rxpquery.NewOptions(
		rxpquery.Limit(boundedOpts.Limit()),
	)
	if len(recs) == int(boundedOpts.Limit()) {
		resOpts = rxpquery.NewOptions(
			rxpquery.ContinueFrom(string(recs[len(recs)-1].Name())),
			rxpquery.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []rxpquery.ResultModifier[*rxpkindversion.KindVersion]{
		rxpquery.ResultWithItems(recs),
		rxpquery.ResultWithOptions[*rxpkindversion.KindVersion](resOpts),
	}
	return rxpquery.NewResult[*rxpkindversion.KindVersion](resNewOpts...), nil
}

// kindversionQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) kindversionQueryValidate(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
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
	opts rxpquery.Options,
) rxpquery.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultKindVersionQueryLimit
	}
	limit = min(limit, MaxKindVersionQueryLimit)
	return rxpquery.NewOptions(rxpquery.Limit(limit))
}
