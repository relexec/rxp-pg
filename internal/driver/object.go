package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/list"
	"github.com/relexec/rxp/list/expression"
	"github.com/relexec/rxp/list/option"
	listoption "github.com/relexec/rxp/list/option"
	"github.com/relexec/rxp/list/result"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/types"
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

	objGen := api.Generation(0) // sel.Generation()
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

// ObjectWrite persists a single supplied Object to backend storage,
func (d *Driver) ObjectWrite(
	ctx context.Context,
	obj *object.Object,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
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
		return err
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
		obj.SetSystem(sys)
	}

	kindRec, err := d.kindStore.ReadByName(ctx, sys, kv.Kind())
	if err != nil {
		return errors.ErrKindVersionUnknown
	}
	err = d.objectWriteValidateScope(ctx, kindRec, obj)
	if err != nil {
		return err
	}

	return d.objectWrite(ctx, obj)
}

// objectWriteValidate returns an error if the supplied object and write
// options are not valid for writing a single Object.
func (d *Driver) objectWriteValidate(
	ctx context.Context,
	obj *object.Object,
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
	obj *object.Object,
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

// objectWrite atomically writes the supplied Object to persistent storage,
func (d *Driver) objectWrite(
	ctx context.Context,
	obj *object.Object,
) error {
	/*
		expectGeneration := opts.Generation()
		if expectGeneration == 0 {
	*/
	// caller expects that they are the first writer of this object. This
	// means we can attempt to insert into the objects table with this
	// object's keys and a generation of 1. any returned unique key
	// contraint violation will indicate another caller tried to create the
	// exact same object concurrently.
	return d.objectStore.WriteFirst(
		ctx, obj,
	)
	/*
		}
			// Otherwise, the caller expects that there is an existing object with this
			// object's keys and that the latest generation of said object matches a
			// supplied generation marker. In this case, we insert a new record into
			// the object_generations table and update the objects table using a WHERE
			// condition against the expected generation. If this UPDATE fails to
			// return any affected rows, we know another caller beat us to write their
			// updated desired state changes and we need to either retry the write or
			// fail.
			return d.objectStore.WriteGeneration(
				ctx, obj, expectGeneration,
			)
	*/
}

const (
	DefaultObjectListLimit = 10
	MaxObjectListLimit     = 100
)

// ObjectList lists zero or more Objects from persistent storage.
func (d *Driver) ObjectList(
	ctx context.Context,
	expr types.Expression,
	opts ...listoption.Option,
) (list.Result[*object.Object], error) {
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
		metrics.InstrumentListRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentListDuration.Record(ctx, elapsed)
	}()

	lopts := listoption.New(opts...)
	err = d.objectListValidate(ctx, expr, lopts)
	if err != nil {
		return nil, err
	}

	boundedOpts := d.objectListBoundedOptions(ctx, lopts)

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
	resOpts := option.New(
		option.WithLimit(boundedOpts.Limit()),
	)
	if len(recs) == boundedOpts.Limit() {
		resOpts = option.New(
			option.WithMarker(recs[len(recs)-1].Object.UUID()),
			option.WithLimit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []result.Option[*object.Object]{
		result.WithItems(objs),
		result.WithOptions[*object.Object](resOpts),
	}
	return result.New[*object.Object](resNewOpts...), nil
}

// objectListValidate returns an error if the supplied expression and list
// options are not valid.
func (d *Driver) objectListValidate(
	ctx context.Context,
	expr types.Expression,
	opts listoption.Options,
) error {
	if expr == nil {
		return errors.ErrListExpressionRequired
	}
	if !expression.ContainsKindPredicate(expr) {
		return errors.ErrInvalidListExpressionKindRequired
	}
	return nil
}

// objectListBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records listed is less
// than the max page result.
func (d *Driver) objectListBoundedOptions(
	ctx context.Context,
	opts listoption.Options,
) listoption.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultObjectListLimit
	}
	limit = min(limit, MaxObjectListLimit)
	return listoption.New(listoption.WithLimit(limit))
}
