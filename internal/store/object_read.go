package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/object/read"
	readoption "github.com/relexec/rxp/object/read/option"
	"github.com/relexec/rxp/object/read/selector"
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
