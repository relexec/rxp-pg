package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/relexec/rxp/api"
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind"
	"github.com/relexec/rxp/kind/kindversion"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/query"

	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storekindversion "github.com/relexec/rxp-pg/internal/store/kindversion"
	storenamespace "github.com/relexec/rxp-pg/internal/store/namespace"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

func isKindishPredicate(p query.Predicate) bool {
	switch p.(type) {
	case
		kind.NamePredicate,
		kind.UUIDPredicate,
		kind.KindPredicate,
		kindversion.KindVersionPredicate,
		kindversion.NamePredicate:
		return true
	default:
		return false
	}
}

// dbReadNamespaceQualifiedByRowID performs a SELECT query to return the stored
// object record having the supplied internal DB RowID with an expected
// namespace-qualified name.
func (s *Store) dbReadNamespaceQualifiedByRowID(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	kvRec *storekindversion.Record,
	nsRec *storenamespace.Record,
	rowID int64,
) (*Record, error) {
	var uuid string
	var name string
	var latestGen api.Generation
	var spec sql.NullString
	out := Record{
		RowID: rowID,
	}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.uuid AS object_uuid
, o.generation AS object_generation
, n.name AS object_name
, o.spec AS object_spec
FROM objects AS o
INNER JOIN namespace_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.namespace = o.namespace
WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND o.id = $4
AND o.namespace = $5
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, kvRec.RowID, kindRec.RowID,
			rowID, nsRec.RowID,
		).Scan(&uuid, &latestGen, &name, &spec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading objects record",
				errors.WithWrap(err),
			)
		}
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithNamespace(nsRec.Namespace),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(latestGen),
		)
		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadDomainQualifiedByRowID performs a SELECT query to return the stored
// object record having the supplied internal DB RowID with an expected
// domain-qualified name.
func (s *Store) dbReadDomainQualifiedByRowID(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	kvRec *storekindversion.Record,
	domRec *storedomain.Record,
	rowID int64,
) (*Record, error) {
	var uuid string
	var name string
	var latestGen api.Generation
	var spec sql.NullString
	out := Record{
		RowID: rowID,
	}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.uuid AS object_uuid
, o.generation AS object_generation
, n.name AS object_name
, o.spec AS object_spec
FROM objects AS o
INNER JOIN domain_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.domain = o.domain
WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND o.id = $4
AND o.domain = $5
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, kvRec.RowID, kindRec.RowID,
			rowID, domRec.RowID,
		).Scan(&uuid, &latestGen, &name, &spec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading objects record",
				errors.WithWrap(err),
			)
		}
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithDomain(domRec.Domain),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(latestGen),
		)
		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadSystemQualifiedByRowID performs a SELECT query to return the stored
// object record having the supplied internal DB RowID with an expected
// system-qualified name.
func (s *Store) dbReadSystemQualifiedByRowID(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	kvRec *storekindversion.Record,
	rowID int64,
) (*Record, error) {
	var uuid string
	var name string
	var latestGen api.Generation
	var spec sql.NullString
	out := Record{
		RowID: rowID,
	}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.uuid AS object_uuid
, o.generation AS object_generation
, n.name AS object_name
, o.spec AS object_spec
FROM objects AS o
INNER JOIN system_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND o.id = $4
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, kvRec.RowID, kindRec.RowID, rowID,
		).Scan(&uuid, &latestGen, &name, &spec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading objects record",
				errors.WithWrap(err),
			)
		}
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(latestGen),
		)
		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadNamespaceQualifiedByUUID performs a SELECT query to return the stored
// object record having the supplied object UUID with an expected
// namespace-qualified name.
func (s *Store) dbReadNamespaceQualifiedByUUID(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	kvRec *storekindversion.Record,
	nsRec *storenamespace.Record,
	uuid string,
) (*Record, error) {
	var name string
	var latestGen api.Generation
	var spec sql.NullString
	out := Record{}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.generation AS object_generation
, n.name AS object_name
, o.spec AS object_spec
FROM objects AS o
INNER JOIN namespace_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.namespace = o.namespace
WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND o.uuid = $4
AND o.namespace = $5
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, kvRec.RowID, kindRec.RowID,
			uuid, nsRec.RowID,
		).Scan(&out.RowID, &latestGen, &name, &spec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading objects record",
				errors.WithWrap(err),
			)
		}
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithNamespace(nsRec.Namespace),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(latestGen),
		)
		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadDomainQualifiedByUUID performs a SELECT query to return the stored
// object record having the supplied object UUID with an expected
// domain-qualified name.
func (s *Store) dbReadDomainQualifiedByUUID(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	kvRec *storekindversion.Record,
	domRec *storedomain.Record,
	uuid string,
) (*Record, error) {
	var name string
	var latestGen api.Generation
	var spec sql.NullString
	var domRowID int64
	out := Record{}
	qargs := []any{
		sysRec.RowID,
		kvRec.RowID,
		kindRec.RowID,
		uuid,
	}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.generation AS object_generation
, n.name AS object_name
, o.spec AS object_spec
, o.domain AS domain_id
FROM objects AS o
INNER JOIN domain_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.domain = o.domain
WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND o.uuid = $4
`
		// NOTE(jaypipes): We allow the lookup of object records by object UUID for
		// domain-qualified objects when we don't have a domain record specified.
		// If domRec is not nil, we add another WHERE condition to ensure that the
		// expected domain is indeed the domain associated with the object.
		if domRec != nil {
			qargs = append(qargs, domRec.RowID)
			qs += "AND o.domain = $5"
		}
		err := tx.QueryRow(ctx, qs, qargs...).Scan(
			&out.RowID,
			&latestGen,
			&name,
			&spec,
			&domRowID,
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
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(latestGen),
		)
		if domRec == nil {
			// We allow looking up a domain-qualified object by object UUID
			// without specifying the Domain. In those cases, we construct the
			// returned Object's Domain here.
			domRec, err = s.domainStore.ReadByRowID(
				ctx, domRowID,
			)
			if err != nil {
				return fmt.Errorf("failed reading domain by row ID: %w", err)
			}
		}
		out.Object.SetDomain(domRec.Domain)
		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadSystemQualifiedByUUID performs a SELECT query to return the stored
// object record having the supplied object UUID with an expected
// system-qualified name.
func (s *Store) dbReadSystemQualifiedByUUID(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	kvRec *storekindversion.Record,
	uuid string,
) (*Record, error) {
	var name string
	var latestGen api.Generation
	var spec sql.NullString
	out := Record{}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.generation AS object_generation
, n.name AS object_name
, o.spec AS object_spec
FROM objects AS o
INNER JOIN system_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND o.uuid = $4
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, kvRec.RowID, kindRec.RowID, uuid,
		).Scan(&out.RowID, &latestGen, &name, &spec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading objects record",
				errors.WithWrap(err),
			)
		}
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(latestGen),
		)
		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadNamespaceQualifiedByName performs a SELECT query to return the stored
// object record having the supplied namespace-qualified name.
func (s *Store) dbReadNamespaceQualifiedByName(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	kvRec *storekindversion.Record,
	nsRec *storenamespace.Record,
	name string,
) (*Record, error) {
	var uuid string
	var latestGen api.Generation
	var spec sql.NullString
	out := Record{}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.uuid AS object_uuid
, o.generation AS object_generation
, o.spec AS object_spec
FROM objects AS o
INNER JOIN namespace_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.namespace = o.namespace
WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND n.name = $4
AND o.namespace = $5
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, kvRec.RowID, kindRec.RowID,
			name, nsRec.RowID,
		).Scan(&out.RowID, &uuid, &latestGen, &spec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading objects record",
				errors.WithWrap(err),
			)
		}
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithNamespace(nsRec.Namespace),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(latestGen),
		)
		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadDomainQualifiedByName performs a SELECT query to return the stored
// object record having the supplied domain-qualified name.
func (s *Store) dbReadDomainQualifiedByName(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	kvRec *storekindversion.Record,
	domRec *storedomain.Record,
	name string,
) (*Record, error) {
	var uuid string
	var latestGen api.Generation
	var spec sql.NullString
	out := Record{}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.uuid AS object_uuid
, o.generation AS object_generation
, o.spec AS object_spec
FROM objects AS o
INNER JOIN domain_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.domain = o.domain
WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND n.name = $4
AND o.domain = $5
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, kvRec.RowID, kindRec.RowID,
			name, domRec.RowID,
		).Scan(&out.RowID, &uuid, &latestGen, &spec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading objects record",
				errors.WithWrap(err),
			)
		}
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithDomain(domRec.Domain),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(latestGen),
		)
		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadSystemQualifiedByName performs a SELECT query to return the stored
// object record having the supplied system-qualified name.
func (s *Store) dbReadSystemQualifiedByName(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	kvRec *storekindversion.Record,
	name string,
) (*Record, error) {
	var uuid string
	var latestGen api.Generation
	var spec sql.NullString
	out := Record{}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.uuid AS object_uuid
, o.generation AS object_generation
, o.spec AS object_spec
FROM objects AS o
INNER JOIN system_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND n.name = $4
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, kvRec.RowID, kindRec.RowID, name,
		).Scan(&out.RowID, &uuid, &latestGen, &spec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading objects record",
				errors.WithWrap(err),
			)
		}
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(latestGen),
		)
		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadAtGeneration grabs object data for a specified generation of the
// object desired state. This method mutates the supplied objectEntry with the
// desired spec for the object at the specific generation.
func (s *Store) dbReadAtGeneration(
	ctx context.Context,
	rowID int64,
	generation api.Generation,
) (*Record, error) {
	var spec sql.NullString
	out := Record{
		RowID: rowID,
		Object: object.New(
			object.WithGeneration(generation),
		),
	}
	fn := func(tx pgx.Tx) error {
		qs := `SELECT spec FROM object_generations WHERE object = $1 AND generation = $2`
		err := tx.QueryRow(ctx, qs, rowID, generation).Scan(&spec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading object_generations record",
				errors.WithWrap(err),
			)
		}

		if spec.Valid {
			out.Object.SetSpec(spec.String)
		}
		return nil
	}
	err := s.Exec(ctx, fn)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// dbInsertFirst is called when the caller believes they are the first
// writer of an object.
func (s *Store) dbInsertFirst(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	kvRec *storekindversion.Record,
	domRec *storedomain.Record,
	nsRec *storenamespace.Record,
	obj object.Object,
) (*object.Object, error) {
	kind := kindRec.Kind
	kv := obj.KindVersionName()
	uuid := obj.UUID()
	name := obj.Name()
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)

	specJSON := obj.Spec()

	var domainRowID *int64
	var nsRowID *int64

	if domRec != nil {
		domainRowID = &domRec.RowID
	}

	if nsRec != nil {
		nsRowID = &nsRec.RowID
	}

	fn := func(tx pgx.Tx) error {
		var objRowID int64
		qs := `
INSERT INTO objects (
  system
, kindversion
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
		err := tx.QueryRow(
			ctx, qs,
			sysRec.RowID,
			kvRec.RowID,
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
					// combinations depending on scope, we check for
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
		scope := kindRec.Kind.Scope()
		switch scope {
		case api.ScopeNamespace:
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
				sysRec.RowID,
				kindRec.RowID,
				nsRec.RowID,
				name,
				createdOn,
				createdBy,
			)
			if err != nil {
				if pgErr, ok := err.(*pgconn.PgError); ok {
					if pgErr.Code == pgerrcode.UniqueViolation {
						qn := fmt.Sprintf(
							"%s:%s:%s",
							nsRec.Namespace.Name(),
							nsRec.Namespace.Domain().Name(),
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
		case api.ScopeDomain:
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
, $7
)`
			_, err = tx.Exec(
				ctx, qs,
				objRowID,
				sysRec.RowID,
				kindRec.RowID,
				domRec.RowID,
				name,
				createdOn,
				createdBy,
			)
			if err != nil {
				if pgErr, ok := err.(*pgconn.PgError); ok {
					if pgErr.Code == pgerrcode.UniqueViolation {
						qn := fmt.Sprintf(
							"%s:%s",
							domRec.Domain.Name(),
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
				sysRec.RowID,
				kindRec.RowID,
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
, kindversion
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
			kvRec.RowID,
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
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	out := obj
	out.SetGeneration(1)
	return &out, nil
}

// dbInsertGeneration is called when the caller believes they are NOT the first
// writer of an object and expect to see a supplied generation.
func (s *Store) dbInsertGeneration(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	kvRec *storekindversion.Record,
	domRec *storedomain.Record,
	nsRec *storenamespace.Record,
	obj object.Object,
	expectGeneration api.Generation,
) (*object.Object, error) {
	kv := obj.KindVersionName()
	uuid := obj.UUID()
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)

	specJSON := obj.Spec()

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
, kindversion
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
			kvRec.RowID,
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
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	out := obj
	out.SetGeneration(expectGeneration + 1)
	return &out, nil
}

type nsqObjectRecord struct {
	ID            int64          `db:"object_id"`
	UUID          string         `db:"object_uuid"`
	Generation    api.Generation `db:"object_generation"`
	Name          string         `db:"object_name"`
	Spec          sql.NullString `db:"object_spec"`
	SystemID      int64          `db:"system_id"`
	KindVersionID int64          `db:"kindversion_id"`
	NamespaceID   int64          `db:"namespace_id"`
}

// dbReadNamespaceQualifiedByExpression queries zero or more Objects that have
// namespace-qualified names from persistent storage given the pre-validated
// expression and options.
func (s *Store) dbReadNamespaceQualifiedByExpression(
	ctx context.Context,
	kv api.KindVersionName,
	kindRec *storekind.Record,
	expr query.Expression,
	opts query.Options,
) ([]*Record, error) {
	sysRec := s.hostSystemRecord
	sys := sysRec.System

	qargs := []any{
		sysRec.RowID,
		kindRec.RowID,
	}
	wheres := []string{
		"o.system = $1",
		"kv.kind = $2",
	}

	kvVerStr := kv.VersionString()
	if kvVerStr != "" {
		wheres = append(wheres, "kv.version = $3")
		qargs = append(qargs, kvVerStr)
	}

	switch expr := expr.(type) {
	case query.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case kind.NamePredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				kn := pred.Value.(api.KindName)
				kindRec, err := s.kindStore.ReadByName(ctx, sys, kn)
				if err != nil {
					return nil, err
				}
				wheres = append(wheres, fmt.Sprintf("kv.kind = $%d", len(qargs)+1))
				qargs = append(qargs, kindRec.RowID)
			}
		}
	case query.OrExpression:
	case query.AndExpression:
	}

	var recs []nsqObjectRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.uuid AS object_uuid
, o.generation AS object_generation
, n.name AS object_name
, o.spec AS object_spec
, o.system AS system_id
, o.kindversion AS kindversion_id
, o.namespace AS namespace_id
FROM objects AS o
 INNER JOIN kindversions AS kv
  ON o.kindversion = kv.id
 INNER JOIN namespace_qualified_object_names AS n
  ON o.id = n.object
`
		if len(wheres) > 0 {
			qs += "\nWHERE " + strings.Join(wheres, " AND ")
		}
		qs += fmt.Sprintf("\nORDER BY o.uuid ASC LIMIT %d", opts.Limit())
		rows, err := tx.Query(ctx, qs, qargs...)
		if err != nil {
			return errors.Internal(
				"failed reading namespace-qualified object records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[nsqObjectRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting namespace-qualified object records",
				errors.WithWrap(err),
			)
		}

		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	out := make([]*Record, 0, len(recs))
	for _, rec := range recs {
		kvRec, err := s.kindversionStore.ReadByRowID(ctx, rec.KindVersionID)
		if err != nil {
			return nil, err
		}
		kv := kvRec.KindVersion.Name()
		nsRec, err := s.namespaceStore.ReadByRowID(ctx, rec.NamespaceID)
		if err != nil {
			return nil, err
		}
		obj := object.New(
			object.WithKindVersionName(kv),
			object.WithUUID(rec.UUID),
			object.WithName(rec.Name),
			object.WithGeneration(rec.Generation),
			object.WithSystem(sys),
			object.WithNamespace(nsRec.Namespace),
		)
		if rec.Spec.Valid {
			obj.SetSpec(rec.Spec.String)
		}
		out = append(out, &Record{
			RowID:  rec.ID,
			Object: obj,
		})
	}
	return out, nil
}

type dqObjectRecord struct {
	ID            int64          `db:"object_id"`
	UUID          string         `db:"object_uuid"`
	Generation    api.Generation `db:"object_generation"`
	Name          string         `db:"object_name"`
	Spec          sql.NullString `db:"object_spec"`
	SystemID      int64          `db:"system_id"`
	KindVersionID int64          `db:"kindversion_id"`
	DomainID      int64          `db:"domain_id"`
}

// dbReadDomainQualifiedByExpression queries zero or more Objects that have
// domain-qualified names from persistent storage given the pre-validated
// expression and options.
func (s *Store) dbReadDomainQualifiedByExpression(
	ctx context.Context,
	kv api.KindVersionName,
	kindRec *storekind.Record,
	expr query.Expression,
	opts query.Options,
) ([]*Record, error) {
	sysRec := s.hostSystemRecord
	sys := sysRec.System

	qargs := []any{
		sysRec.RowID,
		kindRec.RowID,
	}
	wheres := []string{
		"o.system = $1",
		"kv.kind = $2",
	}

	kvVerStr := kv.VersionString()
	if kvVerStr != "" {
		wheres = append(wheres, "kv.version = $3")
		qargs = append(qargs, kvVerStr)
	}

	switch expr := expr.(type) {
	case query.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case kind.NamePredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				kn := pred.Value.(api.KindName)
				kindRec, err := s.kindStore.ReadByName(ctx, sys, kn)
				if err != nil {
					return nil, err
				}
				wheres = append(wheres, fmt.Sprintf("kv.kind = $%d", len(qargs)+1))
				qargs = append(qargs, kindRec.RowID)
			}
		}
	case query.OrExpression:
	case query.AndExpression:
	}

	var recs []dqObjectRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.uuid AS object_uuid
, o.generation AS object_generation
, d.name AS object_name
, o.spec AS object_spec
, o.system AS system_id
, o.kindversion AS kindversion_id
, o.domain AS domain_id
FROM objects AS o
 INNER JOIN kindversions AS kv
  ON o.kindversion = kv.id
 INNER JOIN domain_qualified_object_names AS d
  ON o.id = d.object
`
		if len(wheres) > 0 {
			qs += "\nWHERE " + strings.Join(wheres, " AND ")
		}
		qs += fmt.Sprintf("\nORDER BY o.uuid ASC LIMIT %d", opts.Limit())
		rows, err := tx.Query(ctx, qs, qargs...)
		if err != nil {
			return errors.Internal(
				"failed reading domain-qualified object records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[dqObjectRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting domain-qualified object records",
				errors.WithWrap(err),
			)
		}

		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	out := make([]*Record, 0, len(recs))
	for _, rec := range recs {
		kvRec, err := s.kindversionStore.ReadByRowID(ctx, rec.KindVersionID)
		if err != nil {
			return nil, err
		}
		kv := kvRec.KindVersion.Name()
		domRec, err := s.domainStore.ReadByRowID(ctx, rec.DomainID)
		if err != nil {
			return nil, err
		}
		obj := object.New(
			object.WithKindVersionName(kv),
			object.WithUUID(rec.UUID),
			object.WithName(rec.Name),
			object.WithGeneration(rec.Generation),
			object.WithSystem(sys),
			object.WithDomain(domRec.Domain),
		)
		if rec.Spec.Valid {
			obj.SetSpec(rec.Spec.String)
		}
		out = append(out, &Record{
			RowID:  rec.ID,
			Object: obj,
		})
	}
	return out, nil
}

type sqObjectRecord struct {
	ID            int64          `db:"object_id"`
	UUID          string         `db:"object_uuid"`
	Generation    api.Generation `db:"object_generation"`
	Name          string         `db:"object_name"`
	Spec          sql.NullString `db:"object_spec"`
	SystemID      int64          `db:"system_id"`
	KindVersionID int64          `db:"kindversion_id"`
}

// dbReadSystemQualifiedByExpression queries zero or more Objects that have
// system-qualified names from persistent storage given the pre-validated
// expression and options.
func (s *Store) dbReadSystemQualifiedByExpression(
	ctx context.Context,
	kv api.KindVersionName,
	kindRec *storekind.Record,
	expr query.Expression,
	opts query.Options,
) ([]*Record, error) {
	if query.ContainsPredicate(expr, isKindishPredicate) {
		return nil, errors.ErrInvalidQueryKindPredicate
	}
	sysRec := s.hostSystemRecord
	sys := sysRec.System

	qargs := []any{
		sysRec.RowID,
		kindRec.RowID,
	}
	wheres := []string{
		"o.system = $1",
		"kv.kind = $2",
	}

	kvVerStr := kv.VersionString()
	if kvVerStr != "" {
		wheres = append(wheres, "kv.version = $3")
		qargs = append(qargs, kvVerStr)
	}

	switch expr := expr.(type) {
	case query.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case object.UUIDPredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				u := pred.Value.(string)
				wheres = append(wheres, fmt.Sprintf("o.uuid = $%d", len(qargs)+1))
				qargs = append(qargs, u)
			case query.PredicateOperatorIn:
				us := pred.Value.([]string)
				wheres = append(wheres, fmt.Sprintf("o.uuid = ANY($%d)", len(qargs)+1))
				qargs = append(qargs, us)
			}
		case object.NamePredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				name := pred.Value.(string)
				wheres = append(wheres, fmt.Sprintf("n.name = $%d", len(qargs)+1))
				qargs = append(qargs, name)
			case query.PredicateOperatorIn:
				names := pred.Value.([]string)
				wheres = append(wheres, fmt.Sprintf("n.name = ANY($%d)", len(qargs)+1))
				qargs = append(qargs, names)
			}
		}
	case query.OrExpression:
		subexprs := expr.Expressions()
		ors := make([]string, 0, len(subexprs))
		for _, subexpr := range subexprs {
			switch subexpr := subexpr.(type) {
			case query.UnaryExpression:
				pred := subexpr.Predicate
				switch pred := pred.(type) {
				case object.UUIDPredicate:
					op := pred.Op
					switch op {
					case query.PredicateOperatorEqual:
						u := pred.Value.(string)
						ors = append(ors, fmt.Sprintf("o.uuid = $%d", len(qargs)+1))
						qargs = append(qargs, u)
					case query.PredicateOperatorIn:
						us := pred.Value.([]string)
						ors = append(ors, fmt.Sprintf("o.uuid = ANY($%d)", len(qargs)+1))
						qargs = append(qargs, us)
					}
				case object.NamePredicate:
					op := pred.Op
					switch op {
					case query.PredicateOperatorEqual:
						name := pred.Value.(string)
						ors = append(ors, fmt.Sprintf("n.name = $%d", len(qargs)+1))
						qargs = append(qargs, name)
					case query.PredicateOperatorIn:
						names := pred.Value.([]string)
						ors = append(ors, fmt.Sprintf("n.name = ANY($%d)", len(qargs)+1))
						qargs = append(qargs, names)
					}
				}
			}
		}
		wheres = append(wheres, "("+strings.Join(ors, ") OR (")+")")
	case query.AndExpression:
		subexprs := expr.Expressions()
		ands := make([]string, 0, len(subexprs))
		for _, subexpr := range subexprs {
			switch subexpr := subexpr.(type) {
			case query.UnaryExpression:
				pred := subexpr.Predicate
				switch pred := pred.(type) {
				case object.UUIDPredicate:
					op := pred.Op
					switch op {
					case query.PredicateOperatorEqual:
						u := pred.Value.(string)
						ands = append(ands, fmt.Sprintf("o.uuid = $%d", len(qargs)+1))
						qargs = append(qargs, u)
					}
				case object.NamePredicate:
					op := pred.Op
					switch op {
					case query.PredicateOperatorEqual:
						name := pred.Value.(string)
						ands = append(ands, fmt.Sprintf("n.name = $%d", len(qargs)+1))
						qargs = append(qargs, name)
					}
				}
			}
		}
		wheres = append(wheres, "("+strings.Join(ands, ") AND (")+")")
	}

	var recs []sqObjectRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.uuid AS object_uuid
, o.generation AS object_generation
, n.name AS object_name
, o.spec AS object_spec
, o.system AS system_id
, o.kindversion AS kindversion_id
FROM objects AS o
 INNER JOIN kindversions AS kv
  ON o.kindversion = kv.id
 INNER JOIN system_qualified_object_names AS n
  ON o.id = n.object
`
		if len(wheres) > 0 {
			qs += "WHERE " + strings.Join(wheres, " AND ")
		}
		qs += fmt.Sprintf("\nORDER BY o.uuid ASC LIMIT %d", opts.Limit())
		rows, err := tx.Query(ctx, qs, qargs...)
		if err != nil {
			return errors.Internal(
				"failed reading system-qualified object records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[sqObjectRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting system-qualified object records",
				errors.WithWrap(err),
			)
		}

		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	out := make([]*Record, 0, len(recs))
	for _, rec := range recs {
		kvRec, err := s.kindversionStore.ReadByRowID(ctx, rec.KindVersionID)
		if err != nil {
			return nil, err
		}
		kv := kvRec.KindVersion.Name()
		obj := object.New(
			object.WithKindVersionName(kv),
			object.WithUUID(rec.UUID),
			object.WithName(rec.Name),
			object.WithGeneration(rec.Generation),
			object.WithSystem(sys),
		)
		if rec.Spec.Valid {
			obj.SetSpec(rec.Spec.String)
		}
		out = append(out, &Record{
			RowID:  rec.ID,
			Object: obj,
		})
	}
	return out, nil
}
