package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/namespace"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// NamespaceRead reads a Namespace from persistent storage.
func (d *Driver) NamespaceRead(
	ctx context.Context,
	sel namespace.Selector,
) (*namespace.Namespace, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeNamespace),
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

	err = d.namespaceReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	uuid := sel.UUID()
	if uuid != "" {
		rec, err := d.namespaceStore.ReadByUUID(ctx, uuid)
		if err != nil {
			return nil, err
		}
		return rec.Namespace, nil
	}

	name := sel.Name()
	dom := sel.Domain()
	sys := dom.System()
	if sys == nil {
		dom.SetSystem(d.hostSystemRecord.System)
	}

	rec, err := d.namespaceStore.ReadByName(
		ctx, dom, name,
	)
	if err != nil {
		return nil, err
	}
	return rec.Namespace, nil
}

// namespaceReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Namespace.
func (d *Driver) namespaceReadValidate(
	ctx context.Context,
	sel namespace.Selector,
) error {
	return sel.Validate()
}

// NamespaceWrite atomically writes the supplied Namespace to persistent storage.
func (d *Driver) NamespaceWrite(
	ctx context.Context,
	ns *namespace.Namespace,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeNamespace),
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

	err = d.namespaceWriteValidate(ctx, ns)
	if err != nil {
		return err
	}

	dom := ns.Domain()
	sys := dom.System()
	if sys == nil {
		dom.SetSystem(d.hostSystemRecord.System)
	}
	return d.namespaceStore.Write(ctx, ns)
}

// namespaceWriteValidate returns an error if the supplied namespace and write
// options are not valid for writing a single Namespace.
func (d *Driver) namespaceWriteValidate(
	ctx context.Context,
	ns *namespace.Namespace,
) error {
	if ns == nil {
		return errors.RequiredParameterNil(
			"namespace",
			errors.WithWrap(errors.ErrInvalidWriteRequest),
		)
	}
	return ns.Validate()
}

const (
	DefaultNamespaceQueryLimit = 10
	MaxNamespaceQueryLimit     = 100
)

// NamespaceQuery queries zero or more Namespaces from persistent storage.
func (d *Driver) NamespaceQuery(
	ctx context.Context,
	expr expression.Expression,
	opts ...query.Option,
) (*query.Result[*namespace.Namespace], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeNamespace),
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
	err = d.namespaceQueryValidate(ctx, expr, qopts)
	if err != nil {
		return nil, err
	}

	boundedOpts := d.namespaceQueryBoundedOptions(ctx, qopts)

	recs, err := d.namespaceStore.Query(
		ctx, expr, boundedOpts,
	)
	if err != nil {
		return nil, err
	}
	out := make([]*namespace.Namespace, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.Namespace)
	}
	resOpts := query.NewOptions(
		query.Limit(boundedOpts.Limit()),
	)
	if len(recs) == int(boundedOpts.Limit()) {
		resOpts = query.NewOptions(
			query.ContinueFrom(recs[len(recs)-1].Namespace.UUID()),
			query.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []query.ResultModifier[*namespace.Namespace]{
		query.ResultWithItems(out),
		query.ResultWithOptions[*namespace.Namespace](resOpts),
	}
	return query.NewResult[*namespace.Namespace](resNewOpts...), nil
}

// namespaceQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) namespaceQueryValidate(
	ctx context.Context,
	expr expression.Expression,
	opts query.Options,
) error {
	if expr == nil {
		return errors.ErrQueryExpressionRequired
	}
	return nil
}

// namespaceQueryBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records queryed is less
// than the max page result.
func (d *Driver) namespaceQueryBoundedOptions(
	ctx context.Context,
	opts query.Options,
) query.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultNamespaceQueryLimit
	}
	limit = min(limit, MaxNamespaceQueryLimit)
	return query.NewOptions(query.Limit(limit))
}
