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
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// dbUUIDFromNameDomainQualified returns the UUID associated with the object
// with the supplied name and domain.
func (s *Store) dbUUIDFromNameDomainQualified(
	ctx context.Context,
	sysRec storesystem.Record,
	domRec storedomain.Record,
	name string,
) (string, error) {
	var uuid string
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT o.uuid
FROM domain_qualified_object_names AS n
INNER JOIN objects AS o
 ON n.object = o.id
 AND n.system = o.system
 AND n.domain = o.domain
WHERE n.system = $1
AND n.domain = $2
AND n.name = $3
`
		err := tx.QueryRow(
			ctx, qs,
			sysRec.RowID,
			domRec.RowID,
			name,
		).Scan(&uuid)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading object uuid for domain qualified name",
				errors.WithWrap(err),
			)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return "", err
	}
	return uuid, nil
}

// dbUUIDFromNameSystemQualified returns the UUID associated with the object
// with the supplied name and system.
func (s *Store) dbUUIDFromNameSystemQualified(
	ctx context.Context,
	sysRec storesystem.Record,
	name string,
) (string, error) {
	var uuid string
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT o.uuid
FROM system_qualified_object_names AS n
INNER JOIN objects AS o
 ON n.object = o.id
 AND n.system = o.system
WHERE n.system = $1
AND n.name = $2
`
		err := tx.QueryRow(
			ctx, qs,
			sysRec.RowID,
			name,
		).Scan(&uuid)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading object uuid for system qualified name",
				errors.WithWrap(err),
			)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return "", err
	}
	return uuid, nil
}

const (
	latestSentinel = api.Generation(0)
)

// dbReadByRowIDAndGenerationDomainQualified returns the object record having
// the supplied internal DB RowID and generation with an expected
// domain-qualified name.
func (s *Store) dbReadByRowIDAndGenerationDomainQualified(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kvRec storekindversion.Record,
	domRec storedomain.Record,
	rowID int64,
	requestedGen api.Generation,
) (*Record, error) {
	var uuid string
	var name string
	var generation api.Generation
	var spec sql.NullString
	out := Record{
		RowID: rowID,
	}
	qargs := []any{
		sysRec.RowID,
		kvRec.RowID,
		kindRec.RowID,
		rowID,
		domRec.RowID,
	}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.uuid AS object_uuid
, og.generation AS object_generation
, n.name AS object_name
, og.spec AS object_spec
FROM objects AS o
INNER JOIN domain_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.domain = o.domain
INNER JOIN object_generations AS og
 ON o.id = og.object
`
		if requestedGen == latestSentinel {
			qs += `AND o.generation = og.generation
`
		}
		qs += `
WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND o.id = $4
AND o.domain = $5
`
		if requestedGen != latestSentinel {
			qs += `AND og.generation = $6
`
			qargs = append(qargs, requestedGen)
		}
		err := tx.QueryRow(
			ctx, qs, qargs...,
		).Scan(&uuid, &generation, &name, &spec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domain-qualified objects record by row ID",
				errors.WithWrap(err),
			)
		}
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithDomain(domRec.Domain),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(generation),
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

// dbReadByRowIDAndGenerationSystemQUalified returns the object record having
// the supplied internal DB RowID and generation with an expected
// system-qualified name.
func (s *Store) dbReadByRowIDAndGenerationSystemQualified(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kvRec storekindversion.Record,
	rowID int64,
	requestedGen api.Generation,
) (*Record, error) {
	var uuid string
	var name string
	var generation api.Generation
	var spec sql.NullString
	out := Record{
		RowID: rowID,
	}
	qargs := []any{
		sysRec.RowID,
		kvRec.RowID,
		kindRec.RowID,
		rowID,
	}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.uuid AS object_uuid
, o.generation AS object_generation
, n.name AS object_name
, og.spec AS object_spec
FROM objects AS o
INNER JOIN system_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
INNER JOIN object_generations AS og
 ON o.id = og.object
`
		if requestedGen == latestSentinel {
			qs += `AND o.generation = og.generation
`
		}
		qs += `
WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND o.id = $4
`
		if requestedGen != latestSentinel {
			qs += `AND og.generation = $5
`
			qargs = append(qargs, requestedGen)
		}
		err := tx.QueryRow(ctx, qs, qargs...).Scan(
			&uuid,
			&generation,
			&name,
			&spec,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading system-qualified objects record by row ID",
				errors.WithWrap(err),
			)
		}
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(generation),
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

// dbReadByUUIDAndGenerationDomainQualified returns the object record having
// the supplied object UUID and generation with an expected domain-qualified
// name.
func (s *Store) dbReadByUUIDAndGenerationDomainQualified(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kvRec storekindversion.Record,
	domRec *storedomain.Record,
	uuid string,
	requestedGen api.Generation,
) (*Record, error) {
	var name string
	var generation api.Generation
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
, og.generation AS object_generation
, n.name AS object_name
, og.spec AS object_spec
, o.domain AS domain_id
FROM objects AS o
INNER JOIN domain_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.domain = o.domain
INNER JOIN object_generations AS og
 ON o.id = og.object
`
		if requestedGen == latestSentinel {
			qs += `AND o.generation = og.generation
`
		}
		qs += `WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND o.uuid = $4
`
		if requestedGen != latestSentinel {
			qs += `AND og.generation = $5
`
			qargs = append(qargs, requestedGen)
		}
		// NOTE(jaypipes): We allow the lookup of object records by object UUID for
		// domain-qualified objects when we don't have a domain record specified.
		// If domRec is not nil, we add another WHERE condition to ensure that the
		// expected domain is indeed the domain associated with the object.
		if domRec != nil {
			qs += fmt.Sprintf(`AND o.domain = $%d
`, len(qargs)+1)
			qargs = append(qargs, domRec.RowID)
		}
		err := tx.QueryRow(ctx, qs, qargs...).Scan(
			&out.RowID,
			&generation,
			&name,
			&spec,
			&domRowID,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domain-qualified objects record by UUID",
				errors.WithWrap(err),
			)
		}
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(generation),
		)
		if domRec == nil {
			// We allow looking up a domain-qualified object by object UUID
			// without specifying the Domain. In those cases, we construct the
			// returned Object's Domain here.
			domRec, err = s.domainStore.ReadByRowID(
				ctx, sysRec, domRowID,
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

// dbReadByUUIDAndGenerationSystemQualified return the object record having the
// supplied object UUID and generation with an expected system-qualified name.
func (s *Store) dbReadByUUIDAndGenerationSystemQualified(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kvRec storekindversion.Record,
	uuid string,
	requestedGen api.Generation,
) (*Record, error) {
	var name string
	var generation api.Generation
	var spec sql.NullString
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
, og.spec AS object_spec
FROM objects AS o
INNER JOIN system_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
INNER JOIN object_generations AS og
 ON o.id = og.object
`
		if requestedGen == latestSentinel {
			qs += `AND o.generation = og.generation
`
		}
		qs += `
WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND o.uuid = $4
`
		if requestedGen != latestSentinel {
			qs += `AND og.generation = $5`
			qargs = append(qargs, requestedGen)
		}
		err := tx.QueryRow(ctx, qs, qargs...).Scan(
			&out.RowID,
			&generation,
			&name,
			&spec,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading system-qualified objects record by UUID",
				errors.WithWrap(err),
			)
		}
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(generation),
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

// dbReadByNameAndGenerationDomainQualified returns the object record having
// the supplied domain-qualified name and generation.
func (s *Store) dbReadByNameAndGenerationDomainQualified(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kvRec storekindversion.Record,
	domRec storedomain.Record,
	name string,
	requestedGen api.Generation,
) (*Record, error) {
	var uuid string
	var generation api.Generation
	var spec sql.NullString
	out := Record{}
	qargs := []any{
		sysRec.RowID,
		kvRec.RowID,
		kindRec.RowID,
		name,
		domRec.RowID,
	}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.uuid AS object_uuid
, o.generation AS object_generation
, og.spec AS object_spec
FROM objects AS o
INNER JOIN domain_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.domain = o.domain
INNER JOIN object_generations AS og
 ON o.id = og.object
`
		if requestedGen == latestSentinel {
			qs += `AND o.generation = og.generation
`
		}
		qs += `WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND n.name = $4
AND o.domain = $5
`
		if requestedGen != latestSentinel {
			qs += `AND og.generation = $6`
			qargs = append(qargs, requestedGen)
		}
		err := tx.QueryRow(ctx, qs, qargs...).Scan(
			&out.RowID,
			&uuid,
			&generation,
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
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithDomain(domRec.Domain),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(generation),
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

// dbReadByNameAndGenerationSystemQualified returns the object record having
// the supplied system-qualified name and generation.
func (s *Store) dbReadByNameAndGenerationSystemQualified(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kvRec storekindversion.Record,
	name string,
	requestedGen api.Generation,
) (*Record, error) {
	var uuid string
	var generation api.Generation
	var spec sql.NullString
	out := Record{}
	qargs := []any{
		sysRec.RowID,
		kvRec.RowID,
		kindRec.RowID,
		name,
	}
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
INNER JOIN object_generations AS og
 ON o.id = og.object
`
		if requestedGen == latestSentinel {
			qs += `AND o.generation = og.generation
`
		}
		qs += `WHERE o.system = $1
AND o.kindversion = $2
AND n.kind = $3
AND n.name = $4
`
		if requestedGen != latestSentinel {
			qs += `AND og.generation = $5`
			qargs = append(qargs, requestedGen)
		}
		err := tx.QueryRow(ctx, qs, qargs...).Scan(
			&out.RowID,
			&uuid,
			&generation,
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
		out.Object = object.New(
			object.WithSystem(sysRec.System),
			object.WithKindVersionName(kvRec.KindVersion.Name()),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(generation),
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

// dbInsertFirst is called when the caller believes they are the first
// writer of an object.
func (s *Store) dbInsertFirst(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kvRec storekindversion.Record,
	domRec *storedomain.Record,
	obj object.Object,
) (*object.Object, error) {
	kind := kindRec.Kind
	if kind.Scope() == api.ScopeDomain && domRec == nil {
		return nil, errors.ErrObjectDomainRequired
	}
	kv := obj.KindVersionName()
	uuid := obj.UUID()
	name := obj.Name()
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)

	specJSON := obj.Spec()

	var domainRowID *int64

	if domRec != nil {
		domainRowID = &domRec.RowID
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
) RETURNING id`
		err := tx.QueryRow(
			ctx, qs,
			sysRec.RowID,
			kvRec.RowID,
			uuid,
			1, /* we expect we are the first generation */
			domainRowID,
			createdOn,
			createdBy,
		).Scan(&objRowID)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					// This will be the UUID column uniqueness constraint
					// violation. Since we have different uniqueness
					// constraints for the domain and name combinations
					// depending on scope, we check for name-based collisions
					// before attempting to INSERT a record in the objects
					// table.
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
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kvRec storekindversion.Record,
	domRec *storedomain.Record,
	obj object.Object,
	expectGeneration api.Generation,
) (*object.Object, error) {
	kind := kindRec.Kind
	if kind.Scope() == api.ScopeDomain && domRec == nil {
		return nil, errors.ErrObjectDomainRequired
	}
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
, last_modified_on = $2
, last_modified_by = $3
WHERE id = $4
AND generation = $5`
		res, err := tx.Exec(
			ctx, qs,
			expectGeneration+1, /* we expect we are the first generation */
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
	sysRec storesystem.Record,
	kindRec storekind.Record,
	expr query.Expression,
	opts query.Options,
) ([]*Record, error) {
	if query.ContainsPredicate(expr, isKindishPredicate) {
		return nil, errors.ErrInvalidQueryKindPredicate
	}

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

	var recs []dqObjectRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.uuid AS object_uuid
, og.generation AS object_generation
, d.name AS object_name
, og.spec AS object_spec
, o.system AS system_id
, o.kindversion AS kindversion_id
, o.domain AS domain_id
FROM objects AS o
 INNER JOIN kindversions AS kv
  ON o.kindversion = kv.id
 INNER JOIN domain_qualified_object_names AS d
  ON o.id = d.object
INNER JOIN object_generations AS og
 ON o.id = og.object
 AND o.generation = og.generation
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
		kvName := kv
		if kvVerStr == "" {
			kvRec, err := s.kindversionStore.ReadByRowID(
				ctx, sysRec, kindRec, rec.KindVersionID,
			)
			if err != nil {
				return nil, errors.Internal(
					"failed reading kindversion record",
					errors.WithWrap(err),
				)
			}
			kvName = kvRec.KindVersion.Name()
		}
		domRec, err := s.domainStore.ReadByRowID(
			ctx, sysRec, rec.DomainID,
		)
		if err != nil {
			return nil, err
		}
		obj := object.New(
			object.WithKindVersionName(kvName),
			object.WithUUID(rec.UUID),
			object.WithName(rec.Name),
			object.WithGeneration(rec.Generation),
			object.WithSystem(sysRec.System),
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
	sysRec storesystem.Record,
	kindRec storekind.Record,
	expr query.Expression,
	opts query.Options,
) ([]*Record, error) {
	if query.ContainsPredicate(expr, isKindishPredicate) {
		return nil, errors.ErrInvalidQueryKindPredicate
	}

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
, og.generation AS object_generation
, n.name AS object_name
, og.spec AS object_spec
, o.system AS system_id
, o.kindversion AS kindversion_id
FROM objects AS o
INNER JOIN kindversions AS kv
 ON o.kindversion = kv.id
INNER JOIN system_qualified_object_names AS n
 ON o.id = n.object
INNER JOIN object_generations AS og
 ON o.id = og.object
 AND o.generation = og.generation
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
		kvName := kv
		if kvVerStr == "" {
			kvRec, err := s.kindversionStore.ReadByRowID(
				ctx, sysRec, kindRec, rec.KindVersionID,
			)
			if err != nil {
				return nil, errors.Internal(
					"failed reading kindversion record",
					errors.WithWrap(err),
				)
			}
			kvName = kvRec.KindVersion.Name()
		}
		obj := object.New(
			object.WithKindVersionName(kvName),
			object.WithUUID(rec.UUID),
			object.WithName(rec.Name),
			object.WithGeneration(rec.Generation),
			object.WithSystem(sysRec.System),
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
