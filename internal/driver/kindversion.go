package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind/kindversion"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/query"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// KindVersionRead reads a KindVersion from persistent storage.
func (d *Driver) KindVersionRead(
	ctx context.Context,
	sel kindversion.Selector,
) (*kindversion.KindVersion, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	var name api.KindVersionName

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeKindVersion),
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

	var sysRec *storesystem.Record

	name = sel.Name()
	sys := sel.System()

	// Default the system to the host system if it hasn't been specified in the
	// selector.
	if sys != nil && sys.UUID() != d.hostSystemUUID {
		sysRec, err = d.systemStore.ReadByUUID(ctx, sys.UUID())
		if err != nil {
			if err == errors.ErrNotFound {
				return nil, errors.ErrSystemUnknown
			}
			return nil, err
		}
	} else {
		sysRec = d.hostSystemRecord
	}

	kindRec, err := d.kindStore.ReadByName(ctx, *sysRec, name.Kind())
	if err != nil {
		if err != nil {
			if err == errors.ErrNotFound {
				return nil, errors.ErrKindUnknown
			}
			return nil, err
		}
	}

	rec, err := d.kindversionStore.ReadByName(ctx, *sysRec, *kindRec, name)
	if err != nil {
		return nil, err
	}
	return rec.KindVersion, nil
}

// kindversionReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single KindVersion.
func (d *Driver) kindversionReadValidate(
	ctx context.Context,
	sel kindversion.Selector,
) error {
	return sel.Validate()
}

// KindVersionWrite atomically writes the supplied KindVersion to persistent storage.
func (d *Driver) KindVersionWrite(
	ctx context.Context,
	kv kindversion.KindVersion,
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
			metrics.AttributeType(api.TypeKindVersion),
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

	var sysRec *storesystem.Record

	kn := kv.Name()
	sys := kv.System()

	// Default the system to the host system if it hasn't been specified.
	if sys != nil && sys.UUID() != d.hostSystemUUID {
		sysRec, err = d.systemStore.ReadByUUID(ctx, sys.UUID())
		if err != nil {
			if err == errors.ErrNotFound {
				return errors.ErrSystemUnknown
			}
			return err
		}
	} else {
		sysRec = d.hostSystemRecord
		kv.SetSystem(d.hostSystemRecord.System)
	}

	kindRec, err := d.kindStore.ReadByName(ctx, *sysRec, kn.Kind())
	if err != nil {
		if err != nil {
			if err == errors.ErrNotFound {
				return errors.ErrKindUnknown
			}
			return err
		}
	}
	return d.kindversionStore.Write(ctx, *sysRec, *kindRec, kv)
}

// kindversionWriteValidate returns an error if the supplied kindversion and write
// options are not valid for writing a single KindVersion.
func (d *Driver) kindversionWriteValidate(
	ctx context.Context,
	kv kindversion.KindVersion,
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
) (*query.Result[*kindversion.KindVersion], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeKindVersion),
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
	out := make([]*kindversion.KindVersion, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.KindVersion)
	}
	resOpts := query.NewOptions(
		query.Limit(boundedOpts.Limit()),
	)
	if len(recs) == int(boundedOpts.Limit()) {
		resOpts = query.NewOptions(
			query.ContinueFrom(string(recs[len(recs)-1].KindVersion.Name())),
			query.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []query.ResultModifier[*kindversion.KindVersion]{
		query.ResultWithItems(out),
		query.ResultWithOptions[*kindversion.KindVersion](resOpts),
	}
	return query.NewResult[*kindversion.KindVersion](resNewOpts...), nil
}

// kindversionQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) kindversionQueryValidate(
	ctx context.Context,
	expr query.Expression,
	opts query.Options,
) error {
	if expr == nil {
		return errors.ErrQueryExpressionRequired
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
