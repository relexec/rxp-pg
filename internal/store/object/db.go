package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/relexec/rxp/api"
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"

	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storemeta "github.com/relexec/rxp-pg/internal/store/meta"
	storenamespace "github.com/relexec/rxp-pg/internal/store/namespace"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

var (
	txOptsStrict = pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
	}
)

// dbExec executes the supplied function within the context of a database
// transaction. If the function errors or panics, a ROLLBACK is automatically
// issued for the transaction. If the function completes successfully, a COMMIT
// is automatically issued for the transaction.
func (s *Store) dbExec(
	ctx context.Context,
	fn func(tx pgx.Tx) error,
) error {
	pool := s.pool
	if pool == nil {
		return errors.Internal("connection pool not initialized")
	}
	tx, err := pool.BeginTx(ctx, txOptsStrict)
	if err != nil {
		return errors.Internal(
			fmt.Sprintf("failed beginning transaction"),
			errors.WithWrap(err),
		)
	}

	// make sure we rollback our transaction if a panic occurs.
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback(ctx)
			panic(p)
		}
	}()

	err = fn(tx)
	if err != nil {
		rbErr := tx.Rollback(ctx)
		if rbErr != nil {
			return errors.Internal(
				fmt.Sprintf("failed rolling back transaction"),
				errors.WithWrap(err),
			)
		}
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return errors.Internal(
			fmt.Sprintf("failed committing transaction"),
			errors.WithWrap(err),
		)
	}
	return nil
}

// dbReadNamespaceQualifiedByRowID performs a SELECT query to return the stored
// object record having the supplied internal DB RowID with an expected
// namespace-qualified name.
func (s *Store) dbReadNamespaceQualifiedByRowID(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	metaRec *storemeta.Record,
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
AND o.meta = $2
AND n.kind = $3
AND o.id = $4
AND o.namespace = $5
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, metaRec.RowID, kindRec.RowID,
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
			object.WithKindVersion(metaRec.Meta.KindVersion()),
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
	if err := s.dbExec(ctx, fn); err != nil {
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
	metaRec *storemeta.Record,
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
AND o.meta = $2
AND n.kind = $3
AND o.id = $4
AND o.domain = $5
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, metaRec.RowID, kindRec.RowID,
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
			object.WithKindVersion(metaRec.Meta.KindVersion()),
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
	if err := s.dbExec(ctx, fn); err != nil {
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
	metaRec *storemeta.Record,
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
AND o.meta = $2
AND n.kind = $3
AND o.id = $4
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, metaRec.RowID, kindRec.RowID, rowID,
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
			object.WithKindVersion(metaRec.Meta.KindVersion()),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(latestGen),
		)
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

// dbReadNamespaceQualifiedByUUID performs a SELECT query to return the stored
// object record having the supplied object UUID with an expected
// namespace-qualified name.
func (s *Store) dbReadNamespaceQualifiedByUUID(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	metaRec *storemeta.Record,
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
AND o.meta = $2
AND n.kind = $3
AND o.uuid = $4
AND o.namespace = $5
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, metaRec.RowID, kindRec.RowID,
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
			object.WithKindVersion(metaRec.Meta.KindVersion()),
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
	if err := s.dbExec(ctx, fn); err != nil {
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
	metaRec *storemeta.Record,
	domRec *storedomain.Record,
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
INNER JOIN domain_qualified_object_names AS n
 ON o.id = n.object
 AND o.system = n.system
 AND n.domain = o.domain
WHERE o.system = $1
AND o.meta = $2
AND n.kind = $3
AND o.uuid = $4
AND o.domain = $5
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, metaRec.RowID, kindRec.RowID,
			uuid, domRec.RowID,
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
			object.WithKindVersion(metaRec.Meta.KindVersion()),
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
	if err := s.dbExec(ctx, fn); err != nil {
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
	metaRec *storemeta.Record,
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
AND o.meta = $2
AND n.kind = $3
AND o.uuid = $4
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, metaRec.RowID, kindRec.RowID, uuid,
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
			object.WithKindVersion(metaRec.Meta.KindVersion()),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(latestGen),
		)
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

// dbReadNamespaceQualifiedByName performs a SELECT query to return the stored
// object record having the supplied namespace-qualified name.
func (s *Store) dbReadNamespaceQualifiedByName(
	ctx context.Context,
	sysRec *storesystem.Record,
	kindRec *storekind.Record,
	metaRec *storemeta.Record,
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
AND o.meta = $2
AND n.kind = $3
AND n.name = $4
AND o.namespace = $5
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, metaRec.RowID, kindRec.RowID,
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
			object.WithKindVersion(metaRec.Meta.KindVersion()),
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
	if err := s.dbExec(ctx, fn); err != nil {
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
	metaRec *storemeta.Record,
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
AND o.meta = $2
AND n.kind = $3
AND n.name = $4
AND o.domain = $5
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, metaRec.RowID, kindRec.RowID,
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
			object.WithKindVersion(metaRec.Meta.KindVersion()),
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
	if err := s.dbExec(ctx, fn); err != nil {
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
	metaRec *storemeta.Record,
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
AND o.meta = $2
AND n.kind = $3
AND n.name = $4
`
		err := tx.QueryRow(
			ctx, qs, sysRec.RowID, metaRec.RowID, kindRec.RowID, name,
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
			object.WithKindVersion(metaRec.Meta.KindVersion()),
			object.WithUUID(uuid),
			object.WithName(name),
			object.WithGeneration(latestGen),
		)
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
	err := s.dbExec(ctx, fn)
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
	metaRec *storemeta.Record,
	domRec *storedomain.Record,
	nsRec *storenamespace.Record,
	obj object.Object,
) (*object.Object, error) {
	kind := kindRec.Kind
	kv := obj.KindVersion()
	uuid := obj.UUID()
	name := obj.Name()
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)

	spec := obj.Spec()
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return nil, errors.Internal(
			"failed marshaling spec",
			errors.WithWrap(err),
		)
	}
	specJSON := string(specBytes)

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
			sysRec.RowID,
			metaRec.RowID,
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
			metaRec.RowID,
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
	if err := s.dbExec(ctx, fn); err != nil {
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
	metaRec *storemeta.Record,
	domRec *storedomain.Record,
	nsRec *storenamespace.Record,
	obj object.Object,
	expectGeneration api.Generation,
) (*object.Object, error) {
	kv := obj.KindVersion()
	uuid := obj.UUID()
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)

	spec := obj.Spec()
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return nil, errors.Internal(
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
			metaRec.RowID,
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
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	out := obj
	out.SetGeneration(expectGeneration + 1)
	return &out, nil
}

type objectRecord struct {
	ID          int64          `db:"object_id"`
	UUID        string         `db:"object_uuid"`
	Generation  api.Generation `db:"object_generation"`
	Name        string         `db:"object_name"`
	Spec        sql.NullString `db:"object_spec"`
	SystemID    int64          `db:"system_id"`
	MetaID      int64          `db:"meta_id"`
	NamespaceID int64          `db:"namespace_id"`
}

// dbReadNamespaceQualifiedByExpression queries zero or more Objects that have
// namespace-qualified names from persistent storage given the pre-validated
// expression and options.
func (s *Store) dbReadNamespaceQualifiedByExpression(
	ctx context.Context,
	expr expression.Expression,
	opts query.Options,
) ([]*Record, error) {
	sysRec := s.hostSystemRecord
	sys := sysRec.System

	qargs := []any{
		sysRec.RowID,
	}
	wheres := []string{
		"o.system = $1",
	}

	switch expr := expr.(type) {
	case expression.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case expression.KindNamePredicate:
			op := pred.Operator()
			switch op {
			case expression.PredicateOperatorEqual:
				kn := pred.Value().(api.KindName)
				kindRec, err := s.kindStore.ReadByName(ctx, sys, kn)
				if err != nil {
					return nil, err
				}
				wheres = append(wheres, fmt.Sprintf("m.kind = $%d", len(qargs)+1))
				qargs = append(qargs, kindRec.RowID)
			}
		}
	case expression.OrExpression:
	case expression.AndExpression:
	}

	var recs []objectRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.uuid AS object_uuid
, o.generation AS object_generation
, n.name AS object_name
, o.spec AS object_spec
, o.system AS system_id
, o.meta AS meta_id
, o.namespace AS namespace_id
FROM objects AS o
 INNER JOIN metas AS m
  ON o.meta = m.id
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
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[objectRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting namespace-qualified object records",
				errors.WithWrap(err),
			)
		}

		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	out := make([]*Record, 0, len(recs))
	for _, rec := range recs {
		metaRec, err := s.metaStore.ReadByRowID(ctx, rec.MetaID)
		if err != nil {
			return nil, err
		}
		kv := metaRec.Meta.KindVersion()
		nsRec, err := s.namespaceStore.ReadByRowID(ctx, rec.NamespaceID)
		if err != nil {
			return nil, err
		}
		obj := object.New(
			object.WithKindVersion(kv),
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
