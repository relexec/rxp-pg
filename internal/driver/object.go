package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/object/read"
	readoption "github.com/relexec/rxp/object/read/option"
	"github.com/relexec/rxp/object/read/selector"
	"github.com/relexec/rxp/object/write"
	writeoption "github.com/relexec/rxp/object/write/option"
	"github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storeobject "github.com/relexec/rxp-pg/internal/store/object"
)

// ObjectRead reads a single object from persistent storage.
func (d *Driver) ObjectRead(
	ctx context.Context,
	sel selector.Selector,
	opts ...readoption.Option,
) (types.Object, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	kv := sel.KindVersion()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeTargetType(metrics.TargetTypeObject),
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

	ropts := readoption.New(opts...)
	err = d.objectReadValidate(ctx, sel, ropts)
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

	err = d.objectReadValidateNamescope(ctx, kindRec, sel)
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
	sel selector.Selector,
	opts readoption.Options,
) error {
	return sel.Validate()
}

// objectReadValidateNamescope verifies that the object being read has the
// required namespace and domain in the selector if the namescope of metas is
// either NamescopeNamespace or NamescopeDomain.
func (d *Driver) objectReadValidateNamescope(
	ctx context.Context,
	kindRec *storekind.Record,
	sel selector.Selector,
) error {
	namescope := kindRec.Kind.Namescope()
	switch namescope {
	case types.NamescopeNamespace:
		ns := sel.Namespace()
		if ns == nil {
			return errors.ErrSelectorNamespaceRequired
		}
		return ns.Validate()
	case types.NamescopeDomain:
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
	obj types.Object,
	opts ...writeoption.Option,
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
			metrics.AttributeTargetType(metrics.TargetTypeObject),
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

	wopts := writeoption.New(opts...)
	err = d.objectWriteValidate(ctx, obj, wopts)
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
		obj.(*object.Object).SetSystem(sys)
	}

	kindRec, err := d.kindStore.ReadByName(ctx, sys, kv.Kind())
	if err != nil {
		return errors.ErrKindVersionUnknown
	}
	err = d.objectWriteValidateNamescope(ctx, kindRec, obj)
	if err != nil {
		return err
	}

	return d.objectWrite(ctx, wopts, obj)
}

// objectWriteValidate returns an error if the supplied object and write
// options are not valid for writing a single Object.
func (d *Driver) objectWriteValidate(
	ctx context.Context,
	obj types.Object,
	opts writeoption.Options,
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

// objectWriteValidateNamescope verifies that the object being written has the
// required namespace and domain qualification if the namescope of metas is
// either NamescopeNamespace or NamescopeDomain.
func (d *Driver) objectWriteValidateNamescope(
	ctx context.Context,
	kindRec *storekind.Record,
	obj types.Object,
) error {
	namescope := kindRec.Kind.Namescope()
	switch namescope {
	case types.NamescopeNamespace:
		ns := obj.Namespace()
		if ns == nil {
			return errors.ErrObjectNamespaceRequired
		}
		return ns.Validate()
	case types.NamescopeDomain:
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
	opts writeoption.Options,
	obj types.Object,
) error {
	expectGeneration := opts.Generation()
	if expectGeneration == 0 {
		// caller expects that they are the first writer of this object. This
		// means we can attempt to insert into the objects table with this
		// object's keys and a generation of 1. any returned unique key
		// contraint violation will indicate another caller tried to create the
		// exact same object concurrently.
		return d.objectStore.WriteFirst(
			ctx, obj,
		)
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
}

/*
	kv := obj.KindVersion()
	uuid := obj.UUID()
	name := obj.Name()
	spec := obj.Spec()
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)

	fn := func(tx pgx.Tx) error {
		var specJSON string
		var deltaJSON string
		var existingSpec sql.NullString
		qs := "SELECT id, generation, spec FROM objects WHERE meta = $1 AND meta_generation = $2 AND global_id = $3"
		err := tx.QueryRow(
			ctx, qs,
			metaRec.RowID,
			metaRec.Generation,
			id,
		).Scan(&rec.RowID, &rec.Generation, &existingSpec)
		if err != nil {
			if err != pgx.ErrNoRows {
				return errors.Internal(
					"failed reading objects record",
					errors.WithWrap(err),
				)
			}
			// If the caller specified a generation in the options, that means
			// the caller expects that there should already exist a meta with
			// this kindversion and a matching generation.
			if expectGeneration > 0 {
				return errors.ExpectedToExist(fmt.Sprintf("%s (%s)", kv, id))
			}
			// the delta for a new object can be generated by Diff()'ing the
			// spec against [cmp.Zero].
			delta, err := spec.Diff(cmp.Zero)
			if err != nil {
				return errors.New(
					"failed diffing with empty spec",
					errors.WithCode(errors.ErrCodeBadRequest),
					errors.WithWrap(err),
				)
			}
			if !delta.Different() {
				return errors.New(
					"expected difference between empty spec and supplied spec",
					errors.WithCode(errors.ErrCodeBadRequest),
					errors.WithWrap(err),
				)
			}
			deltaJSONBytes, err := delta.MarshalJSON()
			if err != nil {
				return errors.Internal(
					"failed marshaling delta",
					errors.WithWrap(err),
				)
			}
			deltaJSON = string(deltaJSONBytes)
		}

		if err == nil {
			// If the caller did not specify a generation in the options, that
			// means the caller expects that there should not already exist an
			// object of this kindversion and identifier.
			if expectGeneration == 0 {
				return fmt.Errorf(
					"failed write constraint: expected %s (%s) to not exist",
					kv, id,
				)
			}
			if int64(expectGeneration) != rec.Generation {
				return fmt.Errorf(
					"failed write constraint: "+
						"expected %s (%s) latest generation "+
						"to be %d but found %d",
					kv, id, expectGeneration, rec.Generation,
				)
			}
			if !existingSpec.Valid {
				return fmt.Errorf(
					"failed write constraint: expected %s (%s) to have non-empty spec",
					kv, id,
				)
			}
			existingObj := obj.Meta().NewObject()
			err = existingObj.SpecFrom([]byte(existingSpec.String))
			if err != nil {
				return errors.New(
					"failed building spec from bytes",
					errors.WithCode(errors.ErrCodeBadRequest),
					errors.WithWrap(err),
				)
			}
			existingSpec := existingObj.Spec()
			// the delta for an existing object can be generated by Diff()'ing the
			// new spec against the existing spec.
			delta, err := spec.Diff(existingSpec)
			if err != nil {
				return errors.New(
					"failed diffing with existing spec",
					errors.WithCode(errors.ErrCodeBadRequest),
					errors.WithWrap(err),
				)
			}
			if !delta.Different() {
				return errors.New(
					"expected difference between existing "+
						"spec and supplied spec",
					errors.WithCode(errors.ErrCodeBadRequest),
					errors.WithWrap(err),
				)
			}
			deltaJSONBytes, err := delta.MarshalJSON()
			if err != nil {
				return errors.Internal(
					"failed marshaling delta",
					errors.WithWrap(err),
				)
			}
			deltaJSON = string(deltaJSONBytes)
		}

		specBytes, err := json.Marshal(spec)
		if err != nil {
			return errors.Internal(
				"failed marshaling spec",
				errors.WithWrap(err),
			)
		}
		specJSON = string(specBytes)

		rec.Generation = 1

		qs = "INSERT INTO objects (meta, meta_generation, generation, global_id, domain, namespace, name, spec) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id"
		err = tx.QueryRow(
			ctx, qs,
			metaRec.RowID,
			metaRec.Generation,
			rec.Generation,
			id,
			domRec.RowID,
			nsRec.RowID,
			name,
			specJSON,
		).Scan(&rec.RowID)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.DuplicateID(kv, id)
				}
			}
			return errors.Internal(
				"failed inserting objects record",
				errors.WithWrap(err),
			)
		}
		qs = "INSERT INTO object_generations (object, generation, meta_generation, delta, created_on, created_by) VALUES ($1, $2, $3, $4, $5, $6)"
		_, err = tx.Exec(
			ctx, qs,
			rec.RowID,
			rec.Generation,
			metaRec.Generation,
			deltaJSON,
			createdOn,
			createdBy,
		)
		if err != nil {
			return errors.Internal(
				"failed inserting object_generations record",
				errors.WithWrap(err),
			)
		}
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &rec, nil
}
*/

var _ write.ObjectWriter = (*Driver)(nil)
var _ read.ObjectReader = (*Driver)(nil)
