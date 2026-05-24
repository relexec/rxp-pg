package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storeobject "github.com/relexec/rxp-pg/internal/store/object"
)

// ObjectRead reads a single object from persistent storage.
func (d *Driver) ObjectRead(
	ctx context.Context,
	kv api.KindVersion,
	sel object.Selector,
) (*object.Object, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeObject),
			metrics.AttributeKindVersion(kv),
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

	err = d.objectReadValidate(ctx, kv, sel)
	if err != nil {
		return nil, err
	}

	sys := sel.System()
	dom := sel.Domain()
	ns := sel.Namespace()

	if ns != nil {
		dom = ns.Domain()
		if dom != nil && dom.System() != nil {
			sys = dom.System()
		}
	}

	if ns != nil {
		dom = ns.Domain()
		if dom != nil {
			sys = dom.System()
		}
	} else if dom != nil {
		sys = dom.System()
	}
	// Default the system to the host system if it hasn't been specified.
	if sys == nil {
		sys = d.hostSystemRecord.System
	}

	kindRec, err := d.kindStore.ReadByName(ctx, sys, kv.Kind())
	if err != nil {
		return nil, errors.ErrKindVersionUnknown
	}

	err = d.objectReadValidateScope(ctx, kindRec, sel)
	if err != nil {
		return nil, err
	}

	metaRec, err := d.metaStore.ReadByKindVersion(ctx, sys, kv)
	if err != nil {
		return nil, errors.ErrKindVersionUnknown
	}

	objGen := sel.Generation()
	uuid := sel.UUID()
	name := sel.Name()

	var rec *storeobject.Record
	if uuid != "" {
		rec, err = d.objectStore.ReadByUUID(
			ctx, sys, kindRec.Kind, metaRec.Meta,
			dom, ns, uuid,
		)
	} else {
		rec, err = d.objectStore.ReadByName(
			ctx, sys, kindRec.Kind, metaRec.Meta,
			dom, ns, name,
		)
	}
	if err != nil {
		return nil, err
	}
	if objGen == 0 || objGen == rec.Object.Generation() {
		return rec.Object, nil
	}

	// caller expected a specific generation and it wasn't the latest
	// generation. So we look up a specific generation of the object.
	genRec, err := d.objectStore.ReadAtGeneration(ctx, rec.RowID, objGen)
	if err != nil {
		return nil, err
	}
	rec.Object.SetGeneration(objGen)
	rec.Object.SetSpec(genRec.Object.Spec())
	return rec.Object, nil
}

// objectReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Object.
func (d *Driver) objectReadValidate(
	ctx context.Context,
	kv api.KindVersion,
	sel object.Selector,
) error {
	err := kv.Validate()
	if err != nil {
		return err
	}
	return sel.Validate()
}

// objectReadValidateScope verifies that the object being read has the
// required namespace and domain in the selector if the scope of metas is
// either ScopeNamespace or ScopeDomain.
func (d *Driver) objectReadValidateScope(
	ctx context.Context,
	kindRec *storekind.Record,
	sel object.Selector,
) error {
	if sel.UUID() != "" {
		return nil
	}
	scope := kindRec.Kind.Scope()
	switch scope {
	case api.ScopeNamespace:
		ns := sel.Namespace()
		if ns == nil {
			return errors.ErrSelectorNamespaceRequired
		}
		return ns.Validate()
	case api.ScopeDomain:
		domain := sel.Domain()
		if domain == nil {
			return errors.ErrSelectorDomainRequired
		}
		return domain.Validate()
	}
	return nil
}

// ObjectWrite persists a single supplied Object to backend storage, Note that
// on successful write, the newly-create or updated Object is returned.
func (d *Driver) ObjectWrite(
	ctx context.Context,
	obj object.Object,
) (*object.Object, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	kv := obj.KindVersion()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeObject),
			metrics.AttributeKindVersion(kv),
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

	err = d.objectWriteValidate(ctx, obj)
	if err != nil {
		return nil, err
	}

	sys := obj.System()
	dom := obj.Domain()
	ns := obj.Namespace()

	if ns != nil {
		dom = ns.Domain()
		if dom != nil {
			sys = dom.System()
		}
	} else if dom != nil {
		sys = dom.System()
	}
	// Default the system to the host system if it hasn't been specified.
	if sys == nil {
		sys = d.hostSystemRecord.System
	}
	if obj.System() == nil {
		obj.SetSystem(sys)
	}

	kindRec, err := d.kindStore.ReadByName(ctx, sys, kv.Kind())
	if err != nil {
		return nil, errors.ErrKindVersionUnknown
	}
	err = d.objectWriteValidateScope(ctx, kindRec, obj)
	if err != nil {
		return nil, err
	}
	return d.objectStore.Write(ctx, obj)
}

// objectWriteValidate returns an error if the supplied object and write
// options are not valid for writing a single Object.
func (d *Driver) objectWriteValidate(
	ctx context.Context,
	obj object.Object,
) error {
	kv := obj.KindVersion()
	if kv == "" {
		return errors.ErrObjectKindVersionRequired
	}
	uuid := obj.UUID()
	if uuid == "" {
		return errors.ErrObjectUUIDRequired
	}
	name := obj.Name()
	if name == "" {
		return errors.ErrObjectNameRequired
	}
	return nil
}

// objectWriteValidateScope verifies that the object being written has the
// required namespace and domain qualification if the scope of metas is
// either ScopeNamespace or ScopeDomain.
func (d *Driver) objectWriteValidateScope(
	ctx context.Context,
	kindRec *storekind.Record,
	obj object.Object,
) error {
	scope := kindRec.Kind.Scope()
	switch scope {
	case api.ScopeNamespace:
		ns := obj.Namespace()
		if ns == nil {
			return errors.ErrObjectNamespaceRequired
		}
		return ns.Validate()
	case api.ScopeDomain:
		domain := obj.Domain()
		if domain == nil {
			return errors.ErrObjectDomainRequired
		}
		return domain.Validate()
	}
	return nil
}

const (
	DefaultObjectQueryLimit = 10
	MaxObjectQueryLimit     = 100
)

// ObjectQuery queries zero or more Objects from persistent storage.
func (d *Driver) ObjectQuery(
	ctx context.Context,
	expr expression.Expression,
	opts ...query.Option,
) (*query.Result[*object.Object], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeObject),
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
	err = d.objectQueryValidate(ctx, expr, qopts)
	if err != nil {
		return nil, err
	}

	boundedOpts := d.objectQueryBoundedOptions(ctx, qopts)

	recs, err := d.objectStore.Query(
		ctx, expr, boundedOpts,
	)
	if err != nil {
		return nil, err
	}
	objs := make([]*object.Object, 0, len(recs))
	for _, rec := range recs {
		objs = append(objs, rec.Object)
	}
	resOpts := query.NewOptions(
		query.Limit(boundedOpts.Limit()),
	)
	if len(recs) == boundedOpts.Limit() {
		resOpts = query.NewOptions(
			query.ContinueFrom(recs[len(recs)-1].Object.UUID()),
			query.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []query.ResultModifier[*object.Object]{
		query.ResultWithItems(objs),
		query.ResultWithOptions[*object.Object](resOpts),
	}
	return query.NewResult[*object.Object](resNewOpts...), nil
}

// objectQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) objectQueryValidate(
	ctx context.Context,
	expr expression.Expression,
	opts query.Options,
) error {
	if expr == nil {
		return errors.ErrQueryExpressionRequired
	}
	if !expression.ContainsKindPredicate(expr) {
		return errors.ErrInvalidQueryExpressionKindRequired
	}
	return nil
}

// objectQueryBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records queryed is less
// than the max page result.
func (d *Driver) objectQueryBoundedOptions(
	ctx context.Context,
	opts query.Options,
) query.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultObjectQueryLimit
	}
	limit = min(limit, MaxObjectQueryLimit)
	return query.NewOptions(query.Limit(limit))
}
