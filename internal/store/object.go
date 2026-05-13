package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/object/read"
	readoption "github.com/relexec/rxp/object/read/option"
	"github.com/relexec/rxp/object/read/selector"
	"github.com/relexec/rxp/object/write"
	writeoption "github.com/relexec/rxp/object/write/option"
	rxptypes "github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// objectEntry decorates a Object with internal DB information.
type objectEntry struct {
	// RowID is the internal database BIGSERIAL for the objects record.
	RowID int64
	// Object is the publicly-exposed Object object.
	Object *object.Object
}

// ObjectRead reads a single object from persistent storage.
func (s *Store) ObjectRead(
	ctx context.Context,
	sel selector.Selector,
	opts ...readoption.Option,
) (rxptypes.Object, error) {
	err := s.requestValidate(ctx)
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
	err = s.objectReadValidate(ctx, sel, ropts)
	if err != nil {
		return nil, err
	}

	system := sel.System()
	domain := sel.Domain()
	ns := sel.Namespace()

	if ns != nil {
		domain = ns.Domain()
		if domain != nil && domain.System() != nil {
			system = domain.System()
		}
	}
	// Default the system to the host system if it hasn't been specified.
	if system == nil {
		system = s.hostSystem.System
	}
	systemEntry, err := s.systemRead(ctx, system.UUID())
	if err != nil {
		return nil, err
	}

	kindEntry, err := s.kindRead(ctx, systemEntry, kv.Kind())
	if err != nil {
		return nil, errors.ErrKindVersionUnknown
	}

	err = s.objectReadValidateNamescope(ctx, kindEntry, sel)
	if err != nil {
		return nil, err
	}

	metaEntry, err := s.metaRead(ctx, systemEntry, kindEntry, kv)
	if err != nil {
		return nil, errors.ErrKindVersionUnknown
	}

	var domainEntry *domainEntry
	var nsEntry *namespaceEntry

	if domain == nil && ns != nil {
		domain = ns.Domain()
	}
	if domain != nil {
		domainEntry, err = s.domainRead(ctx, systemEntry, domain.Name())
		if err != nil {
			return nil, err
		}
	}
	if ns != nil {
		if domainEntry == nil {
			return nil, errors.Internal(
				fmt.Sprintf(
					"expected to have domain entry for namespace %q",
					ns.Name(),
				),
			)
		}
		nsEntry, err = s.namespaceRead(ctx, systemEntry, domainEntry, ns.Name())
		if err != nil {
			return nil, err
		}
	}

	objGen := sel.Generation()
	uuid := sel.UUID()
	name := sel.Name()

	entry, err := s.objectRead(
		ctx, kv,
		systemEntry, kindEntry, metaEntry,
		domainEntry, nsEntry,
		name, uuid, objGen,
	)
	if err != nil {
		return nil, err
	}

	// TODO(jaypipes): ensure all read constraints and conditions are met.
	//wantGen := int64(opts.Generation())
	return entry.Object, nil
}

// ObjectWrite persists a single supplied Object to backend storage,
func (s *Store) ObjectWrite(
	ctx context.Context,
	obj rxptypes.Object,
	opts ...writeoption.Option,
) error {
	err := s.requestValidate(ctx)
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
	err = s.objectWriteValidate(ctx, obj, wopts)
	if err != nil {
		return err
	}

	// Before doing anything, we consult our cache of metas to determine if we
	// have seen this type of object before. If not, return an error.

	system := obj.System()
	domain := obj.Domain()
	ns := obj.Namespace()

	if ns != nil {
		domain = ns.Domain()
		if domain != nil {
			system = domain.System()
		}
	} else if domain != nil {
		system = domain.System()
	}
	// Default the system to the host system if it hasn't been specified.
	if system == nil {
		system = s.hostSystem.System
	}
	systemEntry, err := s.systemRead(ctx, system.UUID())
	if err != nil {
		return err
	}

	kindEntry, err := s.kindRead(ctx, systemEntry, kv.Kind())
	if err != nil {
		return errors.ErrKindVersionUnknown
	}

	metaEntry, err := s.metaRead(ctx, systemEntry, kindEntry, kv)
	if err != nil {
		if err == errors.ErrNotFound {
			return errors.ErrKindVersionUnknown
		}
		return err
	}

	err = s.objectWriteValidateNamescope(ctx, kindEntry, obj)
	if err != nil {
		return err
	}

	var domainEntry *domainEntry
	var nsEntry *namespaceEntry

	if domain == nil && ns != nil {
		domain = ns.Domain()
	}
	if domain != nil {
		domainEntry, err = s.domainRead(ctx, systemEntry, domain.Name())
		if err != nil {
			return err
		}
	}
	if ns != nil {
		if domainEntry == nil {
			return errors.Internal(
				fmt.Sprintf(
					"expected to have domain entry for namespace %q",
					ns.Name(),
				),
			)
		}
		nsEntry, err = s.namespaceRead(ctx, systemEntry, domainEntry, ns.Name())
		if err != nil {
			return err
		}
	}

	return s.objectWrite(ctx, wopts, systemEntry, kindEntry, metaEntry, domainEntry, nsEntry, obj)
}

// objectReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Object.
func (s *Store) objectReadValidate(
	ctx context.Context,
	sel selector.Selector,
	opts readoption.Options,
) error {
	return sel.Validate()
}

// objectReadValidateNamescope verifies that the object being read has the
// required namespace and domain in the selector if the namescope of metas is
// either NamescopeNamespace or NamescopeDomain.
func (s *Store) objectReadValidateNamescope(
	ctx context.Context,
	kindEntry *kindEntry,
	sel selector.Selector,
) error {
	namescope := kindEntry.Kind.Namescope()
	switch namescope {
	case rxptypes.NamescopeNamespace:
		ns := sel.Namespace()
		if ns == nil {
			return errors.ErrSelectorNamespaceRequired
		}
		return ns.Validate()
	case rxptypes.NamescopeDomain:
		domain := sel.Domain()
		if domain == nil {
			return errors.ErrSelectorDomainRequired
		}
		return domain.Validate()
	}
	return nil
}

// objectWriteValidate returns an error if the supplied object and write
// options are not valid for writing a single Object.
func (s *Store) objectWriteValidate(
	ctx context.Context,
	obj rxptypes.Object,
	opts writeoption.Options,
) error {
	kv := obj.KindVersion()
	if kv == "" {
		return errors.ErrObjectMissingKindVersion
	}
	uuid := obj.UUID()
	if uuid == "" {
		return errors.ObjectMissingUUID(kv)
	}
	name := obj.Name()
	if name == "" {
		return errors.ObjectMissingName(kv, uuid)
	}
	return nil
}

// objectWriteValidateNamescope verifies that the object being written has the
// required namespace and domain qualification if the namescope of metas is
// either NamescopeNamespace or NamescopeDomain.
func (s *Store) objectWriteValidateNamescope(
	ctx context.Context,
	kindEntry *kindEntry,
	obj rxptypes.Object,
) error {
	namescope := kindEntry.Kind.Namescope()
	switch namescope {
	case rxptypes.NamescopeNamespace:
		ns := obj.Namespace()
		if ns == nil {
			return errors.ErrObjectNamespaceRequired
		}
		return ns.Validate()
	case rxptypes.NamescopeDomain:
		domain := obj.Domain()
		if domain == nil {
			return errors.ErrObjectDomainRequired
		}
		return domain.Validate()
	}
	return nil
}

// objectWrite atomically writes the supplied Object to persistent storage,
func (s *Store) objectWrite(
	ctx context.Context,
	opts writeoption.Options,
	systemEntry *systemEntry,
	kindEntry *kindEntry,
	metaEntry *metaEntry,
	domainEntry *domainEntry,
	nsEntry *namespaceEntry,
	obj rxptypes.Object,
) error {
	expectGeneration := opts.Generation()
	if expectGeneration == 0 {
		// caller expects that they are the first writer of this object. This
		// means we can attempt to insert into the objects table with this
		// object's keys and a generation of 1. any returned unique key
		// contraint violation will indicate another caller tried to create the
		// exact same object concurrently.
		return s.objectWriteFirst(
			ctx, systemEntry, kindEntry, metaEntry, domainEntry, nsEntry, obj,
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
	return s.objectWriteGeneration(
		ctx, systemEntry, kindEntry, metaEntry, domainEntry, nsEntry,
		obj, expectGeneration,
	)
}

// objectWriteFirst is called when the caller believes they are the first
// writer of an object.
func (s *Store) objectWriteFirst(
	ctx context.Context,
	systemEntry *systemEntry,
	kindEntry *kindEntry,
	metaEntry *metaEntry,
	domainEntry *domainEntry,
	nsEntry *namespaceEntry,
	obj rxptypes.Object,
) error {
	kind := kindEntry.Kind
	kv := obj.KindVersion()
	uuid := obj.UUID()
	name := obj.Name()
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)

	spec := obj.Spec()
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return errors.Internal(
			"failed marshaling spec",
			errors.WithWrap(err),
		)
	}
	specJSON := string(specBytes)

	var domainRowID *int64
	var nsRowID *int64

	if domainEntry != nil {
		domainRowID = &domainEntry.RowID
	}

	if nsEntry != nil {
		nsRowID = &nsEntry.RowID
	}

	fn := func(tx pgx.Tx) error {
		var objRowID int64
		qs := `
INSERT INTO objects (
  system
, meta
, uuid
, generation
, domain
, namespace
, spec
, last_modified_on
, last_modified_by
) VALUES (
  $1
, $2
, $3
, $4
, $5
, $6
, $7
, $8
, $9
) RETURNING id`
		err = tx.QueryRow(
			ctx, qs,
			systemEntry.RowID,
			metaEntry.RowID,
			uuid,
			1, /* we expect we are the first generation */
			domainRowID,
			nsRowID,
			specJSON,
			createdOn,
			createdBy,
		).Scan(&objRowID)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					// This will be the UUID column uniqueness constraint
					// violation. Since we have different uniqueness
					// constraints for the domain, namespace and name
					// combinations depending on namescope, we check for
					// name-based collisions before attempting to INSERT a
					// record in the objects table.
					return errors.ExpectedNotToExist(fmt.Sprintf("%s (%s)", kv, uuid))
				}
			}
			return errors.Internal(
				"failed inserting objects record",
				errors.WithWrap(err),
			)
		}
		namescope := kindEntry.Kind.Namescope()
		switch namescope {
		case rxptypes.NamescopeNamespace:
			qs = `
INSERT INTO namespace_qualified_object_names (
  object
, system
, kind
, namespace
, name
, last_modified_on
, last_modified_by
) VALUES (
  $1
, $2
, $3
, $4
, $5
, $6
, $7
)`
			_, err = tx.Exec(
				ctx, qs,
				objRowID,
				systemEntry.RowID,
				kindEntry.RowID,
				nsEntry.RowID,
				name,
				createdOn,
				createdBy,
			)
			if err != nil {
				if pgErr, ok := err.(*pgconn.PgError); ok {
					if pgErr.Code == pgerrcode.UniqueViolation {
						qn := fmt.Sprintf(
							"%s:%s:%s",
							nsEntry.Namespace.Name(),
							nsEntry.Namespace.Domain().Name(),
							name,
						)
						return errors.DuplicateName(kind.Name(), qn)
					}
				}
				return errors.Internal(
					"failed inserting namespace_qualified_object_names record",
					errors.WithWrap(err),
				)
			}
		case rxptypes.NamescopeDomain:
			qs = `
INSERT INTO domain_qualified_object_names (
  object
, system
, kind
, domain
, name
, last_modified_on
, last_modified_by
) VALUES (
  $1
, $2
, $3
, $4
, $5
, $6
)`
			_, err = tx.Exec(
				ctx, qs,
				objRowID,
				systemEntry.RowID,
				kindEntry.RowID,
				domainEntry.RowID,
				name,
				createdOn,
				createdBy,
			)
			if err != nil {
				if pgErr, ok := err.(*pgconn.PgError); ok {
					if pgErr.Code == pgerrcode.UniqueViolation {
						qn := fmt.Sprintf(
							"%s:%s",
							domainEntry.Domain.Name(),
							name,
						)
						return errors.DuplicateName(kind.Name(), qn)
					}
				}
				return errors.Internal(
					"failed inserting domain_qualified_object_names record",
					errors.WithWrap(err),
				)
			}
		default:
			qs = `
INSERT INTO system_qualified_object_names (
  object
, system
, kind
, name
, last_modified_on
, last_modified_by
) VALUES (
  $1
, $2
, $3
, $4
, $5
, $6
)`
			_, err = tx.Exec(
				ctx, qs,
				objRowID,
				systemEntry.RowID,
				kindEntry.RowID,
				name,
				createdOn,
				createdBy,
			)
			if err != nil {
				if pgErr, ok := err.(*pgconn.PgError); ok {
					if pgErr.Code == pgerrcode.UniqueViolation {
						return errors.DuplicateName(kind.Name(), name)
					}
				}
				return errors.Internal(
					"failed inserting system_qualified_object_names record",
					errors.WithWrap(err),
				)
			}
		}
		qs = `
INSERT INTO object_generations (
  object
, generation
, meta
, spec
, created_on
, created_by
) VALUES (
  $1
, $2
, $3
, $4
, $5
, $6
)`
		_, err = tx.Exec(
			ctx, qs,
			objRowID,
			1,
			metaEntry.RowID,
			specJSON,
			createdOn,
			createdBy,
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.ErrConflict
				}
			}
			return errors.Internal(
				"failed inserting object_generations record",
				errors.WithWrap(err),
			)
		}
		return nil
	}
	return s.dbExec(ctx, fn)
}

// objectWriteFirst is called when the caller believes they are the first
// writer of an object.
func (s *Store) objectWriteGeneration(
	ctx context.Context,
	systemEntry *systemEntry,
	kindEntry *kindEntry,
	metaEntry *metaEntry,
	domainEntry *domainEntry,
	nsEntry *namespaceEntry,
	obj rxptypes.Object,
	expectGeneration rxptypes.Generation,
) error {
	kv := obj.KindVersion()
	uuid := obj.UUID()
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)

	spec := obj.Spec()
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return errors.Internal(
			"failed marshaling spec",
			errors.WithWrap(err),
		)
	}
	specJSON := string(specBytes)

	fn := func(tx pgx.Tx) error {
		var objRowID int64
		qs := "SELECT id FROM objects WHERE uuid = $1"
		err := tx.QueryRow(ctx, qs, uuid).Scan(&objRowID)
		if err != nil {
			if err != pgx.ErrNoRows {
				return errors.Internal(
					"failed reading objects record",
					errors.WithWrap(err),
				)
			}
			return errors.ExpectedToExist(
				fmt.Sprintf("%s (%s)", kv, uuid),
			)
		}
		qs = `
INSERT INTO object_generations (
  object
, generation
, meta
, spec
, created_on
, created_by
) VALUES (
  $1
, $2
, $3
, $4
, $5
, $6
)`
		_, err = tx.Exec(
			ctx, qs,
			objRowID,
			expectGeneration+1,
			metaEntry.RowID,
			specJSON,
			createdOn,
			createdBy,
		)
		if err != nil {
			// If we get a unique key violation from the above
			// attempted insert, it means that another thread beat us
			// to update the desired state of this object, so we need
			// to either fail or retry.
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.ErrConflict
				}
			}
			return errors.Internal(
				"failed inserting object_generations record",
				errors.WithWrap(err),
			)
		}
		qs = `
UPDATE objects
SET generation = $1
, spec = $2
, last_modified_on = $3
, last_modified_by = $4
WHERE id = $5
AND generation = $6`
		res, err := tx.Exec(
			ctx, qs,
			expectGeneration+1, /* we expect we are the first generation */
			specJSON,
			createdOn,
			createdBy,
			objRowID,
			expectGeneration,
		)
		if err != nil {
			return errors.Internal(
				"failed updating objects record",
				errors.WithWrap(err),
			)
		}
		// If we get had no rows affected from the above attempted update, it
		// means that another thread beat us to update the desired state of
		// this object, so we need to either fail or retry.
		if res.RowsAffected() == 0 {
			return errors.ErrConflict
		}
		return nil
	}
	return s.dbExec(ctx, fn)
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
			metaEntry.RowID,
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
			domainRec.RowID,
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
// objectRead returns a objectEntry for the supplied pre-validated object
// lookup/selector properties.
func (s *Store) objectRead(
	ctx context.Context,
	kv rxptypes.KindVersion,
	systemEntry *systemEntry,
	kindEntry *kindEntry,
	metaEntry *metaEntry,
	domainEntry *domainEntry,
	nsEntry *namespaceEntry,
	name string,
	uuid string,
	generation rxptypes.Generation,
) (*objectEntry, error) {

	// We do some early checks here for the existence of domains and
	// namespaces. If they were specified in the selector and don't exist, we
	// return early.

	var err error

	kind := kindEntry.Kind

	var entry *objectEntry
	if uuid != "" {
		entry, err = s.objectDBReadByUUID(
			ctx, systemEntry, kindEntry, metaEntry,
			domainEntry, nsEntry,
			uuid,
		)
	} else {
		// We are not looking up based on UUID. Check that the name we were
		// given is appropriately qualified based on the namescope associated
		// with the meta.
		namescope := kind.Namescope()
		switch namescope {
		case rxptypes.NamescopeNamespace:
			if nsEntry == nil {
				return nil, errors.ErrSelectorNamespaceRequired
			}
			entry, err = s.objectDBReadByNamespaceQualifiedName(
				ctx, systemEntry, kindEntry, metaEntry,
				domainEntry, nsEntry, name,
			)
		case rxptypes.NamescopeDomain:
			if domainEntry == nil {
				return nil, errors.ErrSelectorDomainRequired
			}
			entry, err = s.objectDBReadByDomainQualifiedName(
				ctx, systemEntry, kindEntry, metaEntry,
				domainEntry, name,
			)
		default:
			entry, err = s.objectDBReadBySystemQualifiedName(
				ctx, systemEntry, kindEntry, metaEntry, name,
			)
		}
	}
	if err != nil {
		return nil, err
	}
	if generation == 0 || generation == entry.Object.Generation() {
		return entry, nil
	}
	// caller expected a specific generation and it wasn't the latest
	// generation. So we look up a specific generation of the object.
	err = s.objectDBReadAtGeneration(ctx, entry, generation)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

// objectDBReadByUUID grabs object data by its globally-unique string
// identifier while still validating that the supplied optional domain and
// namespace entries point to the correct domain and namespace.
func (s *Store) objectDBReadByUUID(
	ctx context.Context,
	systemEntry *systemEntry,
	kindEntry *kindEntry,
	metaEntry *metaEntry,
	domainEntry *domainEntry,
	nsEntry *namespaceEntry,
	uuid string,
) (*objectEntry, error) {
	var name string
	var latestGen rxptypes.Generation
	var spec sql.NullString
	out := objectEntry{
		Object: object.New(
			object.WithSystem(systemEntry.System),
			object.WithKindVersion(metaEntry.Meta.KindVersion()),
			object.WithUUID(uuid),
		),
	}
	qargs := []any{
		systemEntry.RowID,
		metaEntry.RowID,
		kindEntry.RowID,
		uuid,
	}
	namescope := kindEntry.Kind.Namescope()

	fn := func(tx pgx.Tx) error {
		var qs string
		switch namescope {
		case rxptypes.NamescopeNamespace:
			qs = `
SELECT
  o.id AS object_rowid
, o.generation AS object_generation
, n.name AS object_name
, o.spec AS object_spec
FROM objects AS o
INNER JOIN namespace_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.namespace = o.namespace
WHERE o.system = $1
AND o.meta = $2
AND n.kind = $3
AND o.uuid = $4
AND o.domain = $5
AND o.namespace = $6`
			qargs = append(qargs, nsEntry.DomainRowID)
			qargs = append(qargs, nsEntry.RowID)
		case rxptypes.NamescopeDomain:
			qs = `
SELECT
  o.id AS object_rowid
, o.generation AS object_generation
, n.name AS object_name
, o.spec AS object_spec
FROM objects AS o
INNER JOIN domain_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.domain = o.domain
WHERE o.system = $1
AND o.meta = $2
AND n.kind = $3
AND o.uuid = $4
AND o.domain = $5
AND o.namespace IS NULL`
			qargs = append(qargs, domainEntry.RowID)
		default:
			qs = `
SELECT
  o.id AS object_rowid
, o.generation AS object_generation
, n.name AS object_name
, o.spec AS object_spec
FROM objects AS o
INNER JOIN system_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
WHERE o.system = $1
AND o.meta = $2
AND n.kind = $3
AND o.uuid = $4
AND o.domain IS NULL
AND o.namespace IS NULL`
		}
		err := tx.QueryRow(
			ctx, qs, qargs...,
		).Scan(
			&out.RowID,
			&latestGen,
			&name,
			&spec,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading objects record",
				errors.WithWrap(err),
			)
		}

		out.Object.SetName(name)
		out.Object.SetGeneration(latestGen)

		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}

		if domainEntry != nil {
			out.Object.SetDomain(domainEntry.Domain)
		}
		if nsEntry != nil {
			out.Object.SetNamespace(nsEntry.Namespace)
		}
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// objectDBReadByDomainQualifiedName grabs object data by its name and domain.
func (s *Store) objectDBReadByDomainQualifiedName(
	ctx context.Context,
	systemEntry *systemEntry,
	kindEntry *kindEntry,
	metaEntry *metaEntry,
	domainEntry *domainEntry,
	name string,
) (*objectEntry, error) {

	var uuid string
	var latestGen rxptypes.Generation
	var spec sql.NullString
	out := objectEntry{
		Object: object.New(
			object.WithSystem(systemEntry.System),
			object.WithKindVersion(metaEntry.Meta.KindVersion()),
			object.WithName(name),
			object.WithDomain(domainEntry.Domain),
		),
	}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_rowid
, o.generation AS object_generation
, o.uuid AS object_uuid
, o.spec AS object_spec
FROM objects AS o
INNER JOIN domain_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.domain = o.domain
WHERE o.system = $1
AND o.meta = $2
AND n.kind = $3
AND o.domain = $4
AND o.namespace IS NULL
AND n.name = $5
`
		err := tx.QueryRow(
			ctx, qs,
			systemEntry.RowID, metaEntry.RowID, kindEntry.RowID,
			domainEntry.RowID, name,
		).Scan(
			&out.RowID,
			&latestGen,
			&uuid,
			&spec,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading objects record",
				errors.WithWrap(err),
			)
		}

		out.Object.SetUUID(uuid)
		out.Object.SetGeneration(latestGen)

		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// objectDBReadByNamespaceQualifiedName grabs object data by its name and
// domain/namespace.
func (s *Store) objectDBReadByNamespaceQualifiedName(
	ctx context.Context,
	systemEntry *systemEntry,
	kindEntry *kindEntry,
	metaEntry *metaEntry,
	domainEntry *domainEntry,
	nsEntry *namespaceEntry,
	name string,
) (*objectEntry, error) {
	var uuid string
	var latestGen rxptypes.Generation
	var spec sql.NullString
	out := objectEntry{
		Object: object.New(
			object.WithSystem(systemEntry.System),
			object.WithKindVersion(metaEntry.Meta.KindVersion()),
			object.WithDomain(domainEntry.Domain),
			object.WithNamespace(nsEntry.Namespace),
			object.WithName(name),
		),
	}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_rowid
, o.generation AS object_generation
, o.uuid AS object_uuid
, o.spec AS object_spec
FROM objects AS o
INNER JOIN namespace_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.namespace = o.namespace
WHERE o.system = $1
AND o.meta = $2
AND n.kind = $3
AND o.domain = $4
AND o.namespace = $5
AND n.name = $6
`
		err := tx.QueryRow(
			ctx, qs, systemEntry.RowID, metaEntry.RowID, kindEntry.RowID,
			domainEntry.RowID, nsEntry.RowID, name,
		).Scan(
			&out.RowID,
			&latestGen,
			&uuid,
			&spec,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading objects record",
				errors.WithWrap(err),
			)
		}

		out.Object.SetUUID(uuid)
		out.Object.SetGeneration(latestGen)

		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// objectDBReadBySystemQualifiedName grabs object data by its name.
func (s *Store) objectDBReadBySystemQualifiedName(
	ctx context.Context,
	systemEntry *systemEntry,
	kindEntry *kindEntry,
	metaEntry *metaEntry,
	name string,
) (*objectEntry, error) {

	var uuid string
	var latestGen rxptypes.Generation
	var spec sql.NullString
	out := objectEntry{
		Object: object.New(
			object.WithSystem(systemEntry.System),
			object.WithKindVersion(metaEntry.Meta.KindVersion()),
			object.WithName(name),
		),
	}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_rowid
, o.generation AS object_generation
, o.uuid AS object_uuid
, o.spec AS object_spec
FROM objects AS o
INNER JOIN system_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
WHERE o.system = $1
AND o.meta = $2
AND n.kind = $3
AND o.name = $4
AND o.domain IS NULL
AND o.namespace IS NULL
`
		err := tx.QueryRow(
			ctx, qs, systemEntry.RowID, metaEntry.RowID, kindEntry.RowID,
			name,
		).Scan(
			&out.RowID,
			&latestGen,
			&uuid,
			&spec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading objects record",
				errors.WithWrap(err),
			)
		}

		out.Object.SetUUID(uuid)
		out.Object.SetGeneration(latestGen)

		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// objectDBReadAtGeneration grabs object data for a specified generation of the
// object desired state. This method mutates the supplied objectEntry with the
// desired spec for the object at the specific generation.
func (s *Store) objectDBReadAtGeneration(
	ctx context.Context,
	entry *objectEntry,
	generation rxptypes.Generation,
) error {
	var spec sql.NullString
	fn := func(tx pgx.Tx) error {
		qs := `SELECT spec FROM object_generations WHERE object = $1 AND generation = $2`
		err := tx.QueryRow(ctx, qs, entry.RowID, generation).Scan(&spec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading object_generations record",
				errors.WithWrap(err),
			)
		}

		entry.Object.SetGeneration(generation)

		if spec.Valid {
			entry.Object.SetSpec(spec.String)
		}
		return nil
	}
	return s.dbExec(ctx, fn)
}

var _ read.ObjectReader = (*Store)(nil)
var _ write.ObjectWriter = (*Store)(nil)
