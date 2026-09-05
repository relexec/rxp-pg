package driver

import (
	"context"
	"time"

	apicore "github.com/relexec/rxp/api/core"
	rxpdomain "github.com/relexec/rxp/api/domain"
	apierrors "github.com/relexec/rxp/api/errors"
	rxpkind "github.com/relexec/rxp/api/kind"
	rxpkindversion "github.com/relexec/rxp/api/kindversion"
	rxpobject "github.com/relexec/rxp/api/object"
	rxpquery "github.com/relexec/rxp/api/query"
	rxpsystem "github.com/relexec/rxp/api/system"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	storeobject "github.com/relexec/rxp-pg/internal/store/object"
	"github.com/relexec/rxp-pg/metrics"
)

// ObjectRead reads a single Object from persistent storage.
func (d *Driver) ObjectRead(
	ctx context.Context,
	kv rxpkindversion.Name,
	sel rxpobject.Selector,
) (*rxpobject.Object, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(apicore.TypeObject),
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
	//
	// We also want to ensure that the KindVersion they specified in the
	// ObjectRead call matches the KindVersion associated with the object
	// record itself.

	sys := sel.System()
	dom := sel.Domain()

	if dom != nil {
		sys = dom.System
	}

	sysRec, err := d.systemRecordFromSystem(ctx, sys)
	if err != nil {
		return nil, err
	}

	kindRec, err := d.kindStore.ReadByName(ctx, sysRec, kv.Kind())
	if err != nil {
		if err == apierrors.ErrNotFound {
			return nil, apierrors.ErrKindUnknown
		}
		return nil, err
	}

	err = d.objectReadValidateScope(ctx, kindRec, sel)
	if err != nil {
		return nil, err
	}

	kvRec, err := d.kindversionStore.ReadByName(ctx, sysRec, kindRec, kv)
	if err != nil {
		if err == apierrors.ErrNotFound {
			return nil, apierrors.ErrKindVersionUnknown
		}
		return nil, err
	}

	domRec, err := d.domainRecordFromDomain(ctx, sysRec, dom)
	if err != nil {
		return nil, err
	}

	objGen := sel.Generation()
	name := sel.Name()
	uuid := sel.UUID()

	var rec *storeobject.Record
	if uuid == "" {
		uuid, err = d.objectUUIDFromName(ctx, sysRec, kindRec, name, domRec)
		if err != nil {
			return nil, err
		}
	} else {
		name, err = d.objectNameFromUUID(ctx, sysRec, kindRec, uuid, domRec)
		if err != nil {
			return nil, err
		}
	}
	rec, err = d.objectStore.ReadByUUIDAndGeneration(ctx, kvRec, uuid, objGen)
	if err != nil {
		return nil, err
	}
	rec.Object.System = sysRec
	if dom != nil {
		rec.Object.Domain = dom
	}
	rec.Object.KindVersionName = kvRec.Name()
	rec.Object.Name = name
	return rec.Object, nil
}

// objectReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Object.
func (d *Driver) objectReadValidate(
	ctx context.Context,
	kv rxpkindversion.Name,
	sel rxpobject.Selector,
) error {
	err := kv.Validate()
	if err != nil {
		return err
	}
	return sel.Validate()
}

// objectUUIDFromName returns the object's UUID given a system record, kind
// record, name and optional domain record.
func (d *Driver) objectUUIDFromName(
	ctx context.Context,
	sysRec *rxpsystem.System,
	kindRec *rxpkind.Kind,
	name string,
	domRec *rxpdomain.Domain,
) (string, error) {
	qualifier := storeobject.NameQualifier{
		System: sysRec,
	}
	if kindRec.Scope == apicore.ScopeDomain {
		qualifier.Domain = domRec
	}
	return d.objectStore.UUIDFromName(
		ctx, name, qualifier,
	)
}

// objectNameFromUUID returns the object's name given a system record, kind
// record, object UUID and optional domain record.
func (d *Driver) objectNameFromUUID(
	ctx context.Context,
	sysRec *rxpsystem.System,
	kindRec *rxpkind.Kind,
	uuid string,
	domRec *rxpdomain.Domain,
) (string, error) {
	qualifier := storeobject.NameQualifier{
		System: sysRec,
	}
	if kindRec.Scope == apicore.ScopeDomain {
		qualifier.Domain = domRec
	}
	return d.objectStore.NameFromUUID(
		ctx, uuid, qualifier,
	)
}

// objectReadValidateScope verifies that the object being read has the required
// domain in the selector if the scope of Kind is ScopeDomain.
func (d *Driver) objectReadValidateScope(
	ctx context.Context,
	kindRec *rxpkind.Kind,
	sel rxpobject.Selector,
) error {
	scope := kindRec.Scope
	switch scope {
	case apicore.ScopeDomain:
		domain := sel.Domain()
		if domain == nil {
			return apierrors.ErrSelectorDomainRequired
		}
	}
	return nil
}

// ObjectWrite persists a single supplied Object to backend storage, Note that
// on successful write, the newly-created or updated Object is returned.
func (d *Driver) ObjectWrite(
	ctx context.Context,
	obj rxpobject.Object,
) (*rxpobject.Object, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	kv := obj.KindVersionName

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(apicore.TypeObject),
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

	sys := obj.System
	dom := obj.Domain

	if dom != nil {
		sys = dom.System
	}

	// Default the system to the host system if it hasn't been specified.
	if sys == nil {
		sys = d.hostSystemRecord
		if dom != nil {
			dom.System = sys
		}
	}

	sysRec, err := d.systemRecordFromSystem(ctx, sys)
	if err != nil {
		return nil, err
	}

	kindRec, err := d.kindStore.ReadByName(ctx, sysRec, kv.Kind())
	if err != nil {
		if err == apierrors.ErrNotFound {
			return nil, apierrors.ErrKindUnknown
		}
		return nil, err
	}

	err = d.objectWriteValidateScope(ctx, kindRec, obj)
	if err != nil {
		return nil, err
	}

	kvRec, err := d.kindversionStore.ReadByName(ctx, sysRec, kindRec, kv)
	if err != nil {
		if err == apierrors.ErrNotFound {
			return nil, apierrors.ErrKindUnknown
		}
		return nil, err
	}

	domRec, err := d.domainRecordFromDomain(ctx, sysRec, dom)
	if err != nil {
		return nil, err
	}
	return d.objectStore.Write(ctx, sysRec, kindRec, kvRec, domRec, obj)
}

// objectWriteValidate returns an error if the supplied object and write
// options are not valid for writing a single Object.
func (d *Driver) objectWriteValidate(
	ctx context.Context,
	obj rxpobject.Object,
) error {
	kv := obj.KindVersionName
	if kv == "" {
		return apierrors.ErrObjectKindVersionRequired
	}
	uuid := obj.UUID
	if uuid == "" {
		return apierrors.ErrObjectUUIDRequired
	}
	name := obj.Name
	if name == "" {
		return apierrors.ErrObjectNameRequired
	}
	return nil
}

// objectWriteValidateScope verifies that the object being written has the
// required domain qualification if the scope of Kind is ScopeDomain.
func (d *Driver) objectWriteValidateScope(
	ctx context.Context,
	kindRec *rxpkind.Kind,
	obj rxpobject.Object,
) error {
	if kindRec.Scope == apicore.ScopeDomain {
		dom := obj.Domain
		if dom == nil {
			return apierrors.ErrObjectDomainRequired
		}
		return dom.Validate()
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
	kv rxpkindversion.Name,
	expr rxpquery.Expression,
	opts ...rxpquery.Option,
) (*rxpquery.Result[*rxpobject.Object], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(apicore.TypeObject),
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

	qopts := rxpquery.NewOptions(opts...)
	err = d.objectQueryValidate(ctx, kv, expr, qopts)
	if err != nil {
		return nil, err
	}

	sysRec := d.hostSystemRecord

	kindRec, err := d.kindStore.ReadByName(ctx, sysRec, kv.Kind())
	if err != nil {
		if err == apierrors.ErrNotFound {
			return nil, apierrors.ErrKindUnknown
		}
		return nil, err
	}

	boundedOpts := d.objectQueryBoundedOptions(ctx, qopts)

	recs, err := d.objectStore.Query(
		ctx, kv, sysRec, kindRec, expr, boundedOpts,
	)
	if err != nil {
		return nil, err
	}
	objs := make([]*rxpobject.Object, 0, len(recs))
	for _, rec := range recs {
		objs = append(objs, rec.Object)
	}
	resNewOpts := []rxpquery.ResultModifier[*rxpobject.Object]{
		rxpquery.ResultWithItems(objs),
		rxpquery.ResultWithOptions[*rxpobject.Object](boundedOpts),
	}
	if len(recs) == int(boundedOpts.Limit()) {
		resNewOpts = append(
			resNewOpts,
			rxpquery.ResultWithMarker[*rxpobject.Object](
				recs[len(recs)-1].Object.UUID,
			),
		)
	}
	return rxpquery.NewResult[*rxpobject.Object](resNewOpts...), nil
}

// objectQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) objectQueryValidate(
	ctx context.Context,
	kv rxpkindversion.Name,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) error {
	return kv.Validate()
}

// objectQueryBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records queryed is less
// than the max page result.
func (d *Driver) objectQueryBoundedOptions(
	ctx context.Context,
	opts rxpquery.Options,
) rxpquery.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultObjectQueryLimit
	}
	limit = min(limit, MaxObjectQueryLimit)
	return rxpquery.NewOptions(rxpquery.Limit(limit))
}
