package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/query"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storeobject "github.com/relexec/rxp-pg/internal/store/object"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// ObjectRead reads a single Object from persistent storage.
func (d *Driver) ObjectRead(
	ctx context.Context,
	kv api.KindVersionName,
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

	// NOTE(jaypipes): We will always look up Objects by UUID. If the user has
	// not specified a UUID in the selector, we will ask the object store to
	// fetch the UUID associated with the Object's name. In order to do that,
	// however, we need to first identify whether the Kind of the Object is
	// domain or system-scoped.
	//
	// Even if the user specified a UUID for the Object, we still want to look
	// up the name associated with the UUID and verify that the system/domain
	// specified for the Object is valid and matches the system/domain we had
	// stored for the Object with that UUID.

	var sysRec *storesystem.Record
	var domRec *storedomain.Record

	sys := sel.System()
	dom := sel.Domain()

	if dom != nil {
		sys = dom.System()
	}

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

	kindRec, err := d.kindStore.ReadByName(ctx, *sysRec, kv.Kind())
	if err != nil {
		if err != nil {
			if err == errors.ErrNotFound {
				return nil, errors.ErrKindUnknown
			}
			return nil, err
		}
	}

	err = d.objectReadValidateScope(ctx, kindRec, sel)
	if err != nil {
		return nil, err
	}

	kvRec, err := d.kindversionStore.ReadByName(ctx, *sysRec, *kindRec, kv)
	if err != nil {
		if err != nil {
			if err == errors.ErrNotFound {
				return nil, errors.ErrKindVersionUnknown
			}
			return nil, err
		}
	}

	if dom != nil {
		if dom.UUID() != "" {
			domRec, err = d.domainStore.ReadByUUID(
				ctx, *sysRec, dom.UUID(),
			)
		} else {
			domRec, err = d.domainStore.ReadByName(
				ctx, *sysRec, dom.Name(),
			)
		}
		if err != nil {
			return nil, err
		}
	}

	objGen := sel.Generation()
	uuid := sel.UUID()
	name := sel.Name()

	var rec *storeobject.Record
	if uuid == "" {
		qualifier := storeobject.NameQualifier{
			System: *sysRec,
		}
		if kindRec.Kind.Scope() == api.ScopeDomain {
			qualifier.Domain = domRec
			uuid, err = d.objectStore.UUIDFromName(
				ctx, name, qualifier,
			)
		} else {
			uuid, err = d.objectStore.UUIDFromName(
				ctx, name, qualifier,
			)
		}
		if err != nil {
			return nil, err
		}
	}
	rec, err = d.objectStore.ReadByUUIDAndGeneration(
		ctx, *sysRec, *kindRec, *kvRec, domRec, uuid, objGen,
	)
	if err != nil {
		return nil, err
	}
	return rec.Object, nil
}

// objectReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Object.
func (d *Driver) objectReadValidate(
	ctx context.Context,
	kv api.KindVersionName,
	sel object.Selector,
) error {
	err := kv.Validate()
	if err != nil {
		return err
	}
	return sel.Validate()
}

// objectReadValidateScope verifies that the object being read has the required
// domain in the selector if the scope of Kind is ScopeDomain.
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
// on successful write, the newly-created or updated Object is returned.
func (d *Driver) ObjectWrite(
	ctx context.Context,
	obj object.Object,
) (*object.Object, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	kv := obj.KindVersionName()

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

	var sysRec *storesystem.Record
	var domRec *storedomain.Record

	sys := obj.System()
	dom := obj.Domain()

	if dom != nil {
		sys = dom.System()
	}

	// Default the system to the host system if it hasn't been specified.
	if sys == nil {
		sys = d.hostSystemRecord.System
		if dom != nil {
			dom.SetSystem(sys)
		}
	}

	if sys.UUID() != d.hostSystemUUID {
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

	kindRec, err := d.kindStore.ReadByName(ctx, *sysRec, kv.Kind())
	if err != nil {
		if err != nil {
			if err == errors.ErrNotFound {
				return nil, errors.ErrKindUnknown
			}
			return nil, err
		}
	}

	err = d.objectWriteValidateScope(ctx, *kindRec, obj)
	if err != nil {
		return nil, err
	}

	if kindRec.Kind.Scope() == api.ScopeDomain {
		domRec, err = d.domainStore.ReadByUUID(ctx, *sysRec, dom.UUID())
		if err != nil {
			return nil, err
		}
	}

	kvRec, err := d.kindversionStore.ReadByName(ctx, *sysRec, *kindRec, kv)
	if err != nil {
		if err != nil {
			if err == errors.ErrNotFound {
				return nil, errors.ErrKindUnknown
			}
			return nil, err
		}
	}
	return d.objectStore.Write(ctx, *sysRec, *kindRec, *kvRec, domRec, obj)
}

// objectWriteValidate returns an error if the supplied object and write
// options are not valid for writing a single Object.
func (d *Driver) objectWriteValidate(
	ctx context.Context,
	obj object.Object,
) error {
	kv := obj.KindVersionName()
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
// required domain qualification if the scope of Kind is ScopeDomain.
func (d *Driver) objectWriteValidateScope(
	ctx context.Context,
	kindRec storekind.Record,
	obj object.Object,
) error {
	if kindRec.Kind.Scope() == api.ScopeDomain {
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

// ObjectQuery queries zero or more Objects of a specified kind or kindversion
// from persistent storage.
func (d *Driver) ObjectQuery(
	ctx context.Context,
	kv api.KindVersionName,
	expr query.Expression,
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
			metrics.AttributeKindVersion(kv),
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
	err = d.objectQueryValidate(ctx, kv, expr, qopts)
	if err != nil {
		return nil, err
	}

	sysRec := d.hostSystemRecord

	kindRec, err := d.kindStore.ReadByName(ctx, *sysRec, kv.Kind())
	if err != nil {
		if err != nil {
			if err == errors.ErrNotFound {
				return nil, errors.ErrKindUnknown
			}
			return nil, err
		}
	}

	boundedOpts := d.objectQueryBoundedOptions(ctx, qopts)

	recs, err := d.objectStore.Query(
		ctx, kv, *sysRec, *kindRec, expr, boundedOpts,
	)
	if err != nil {
		return nil, err
	}
	objs := make([]*object.Object, 0, len(recs))
	for _, rec := range recs {
		objs = append(objs, rec.Object)
	}
	resNewOpts := []query.ResultModifier[*object.Object]{
		query.ResultWithItems(objs),
		query.ResultWithOptions[*object.Object](boundedOpts),
	}
	if len(recs) == int(boundedOpts.Limit()) {
		resNewOpts = append(
			resNewOpts,
			query.ResultWithMarker[*object.Object](
				recs[len(recs)-1].Object.UUID(),
			),
		)
	}
	return query.NewResult[*object.Object](resNewOpts...), nil
}

// objectQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) objectQueryValidate(
	ctx context.Context,
	kv api.KindVersionName,
	expr query.Expression,
	opts query.Options,
) error {
	if err := kv.Validate(); err != nil {
		return err
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
