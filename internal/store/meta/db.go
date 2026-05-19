package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/meta"
	"github.com/relexec/rxp/meta/schema"
	"github.com/relexec/rxp/types"
	"github.com/relexec/rxp/version"

	storekind "github.com/relexec/rxp-pg/internal/store/kind"
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

// dbReadByRowID performs a SELECT query to return the stored meta record
// having the supplied internal DB RowID.
func (s *Store) dbReadByRowID(
	ctx context.Context,
	rowID int64,
) (*Record, error) {
	var systemRowID int64
	var kindRowID int64
	var verStr string
	var schemaBytes sql.NullString
	var schema schema.Schema
	out := Record{
		RowID: rowID,
	}
	fn := func(tx pgx.Tx) error {
		qs := "SELECT system, kind, version, schema FROM metas WHERE id = $1"
		err := tx.QueryRow(
			ctx, qs, rowID,
		).Scan(
			&systemRowID, &kindRowID, &verStr,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading metas record",
				errors.WithWrap(err),
			)
		}
		systemRec, err := s.systemStore.ReadByRowID(ctx, systemRowID)
		if err != nil {
			return errors.Internal(
				"failed reading system record for meta",
				errors.WithWrap(err),
			)
		}
		kindRec, err := s.kindStore.ReadByRowID(ctx, kindRowID)
		if err != nil {
			return errors.Internal(
				"failed reading kind record for meta",
				errors.WithWrap(err),
			)
		}
		if schemaBytes.Valid {
			err = json.Unmarshal([]byte(schemaBytes.String), &schema)
			if err != nil {
				return errors.Internal(
					"failed unmarshaling meta schema",
					errors.WithWrap(err),
				)
			}
		}
		sv, err := semver.NewVersion(verStr)
		if err != nil {
			return errors.Internal(
				"failed parsing semver for meta",
				errors.WithWrap(err),
			)
		}
		out.Meta = meta.New(
			meta.WithSystem(systemRec.System),
			meta.WithKind(kindRec.Kind),
			meta.WithVersion(*sv),
			meta.WithSchema(&schema),
		)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByName performs a SELECT query to return the stored meta record
// having the supplied KindVersion.
func (s *Store) dbReadByKindVersion(
	ctx context.Context,
	systemRec *storesystem.Record,
	kindRec *storekind.Record,
	kv types.KindVersion,
) (*Record, error) {
	sv, _ := kv.Version()
	verStr := kv.VersionString()
	var schemaBytes sql.NullString
	var schema schema.Schema
	out := Record{}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  id
, schema
FROM metas
WHERE system = $1
AND kind = $2
AND version = $3
`
		err := tx.QueryRow(
			ctx, qs,
			systemRec.RowID, kindRec.RowID, verStr,
		).Scan(
			&out.RowID, &schemaBytes,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading metas record",
				errors.WithWrap(err),
			)
		}
		if schemaBytes.Valid {
			err = json.Unmarshal([]byte(schemaBytes.String), &schema)
			if err != nil {
				return errors.Internal(
					"failed unmarshaling meta schema",
					errors.WithWrap(err),
				)
			}
		}
		out.Meta = meta.New(
			meta.WithSystem(systemRec.System),
			meta.WithKind(kindRec.Kind),
			meta.WithVersion(*sv),
			meta.WithSchema(&schema),
		)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbVersionsForKind returns a version.Set representing all the semantic
// versions known for the supplied Kind.
func (s *Store) dbVersionsForKind(
	ctx context.Context,
	systemRec *storesystem.Record,
	kindRec *storekind.Record,
) (version.Set, error) {
	var versionStrs []string
	fn := func(tx pgx.Tx) error {
		qs := "SELECT version FROM metas WHERE system = $1 AND kind = $2"
		rows, err := tx.Query(ctx, qs, systemRec.RowID, kindRec.RowID)
		if err != nil {
			return errors.Internal(
				"failed reading meta records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		versionStrs, err = pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return errors.Internal(
				"failed collecting meta versions",
				errors.WithWrap(err),
			)
		}

		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	versions := []semver.Version{}
	for _, verStr := range versionStrs {
		v, err := semver.NewVersion(verStr)
		if err != nil {
			return nil, errors.Internal(
				"failed parsing semver",
				errors.WithWrap(err),
			)
		}
		versions = append(versions, *v)
	}
	vs := version.Set{}
	vs.Add(versions...)
	return vs, nil
}

// dbInsert atomically writes the supplied Meta to persistent storage.
func (s *Store) dbInsert(
	ctx context.Context,
	systemRec *storesystem.Record,
	kindRec *storekind.Record,
	m types.Meta,
) error {
	kv := m.KindVersion()
	ver, _ := kv.Version()
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	schemaJSON, err := m.SchemaJSON()
	if err != nil {
		return err
	}
	fn := func(tx pgx.Tx) error {
		versions, err := s.dbVersionsForKind(ctx, systemRec, kindRec)
		if err != nil {
			return err
		}
		if len(versions) == 0 {
			// ensure we were given the first version in the version series OR
			// there was a force override option.
			if ver.Minor() != 0 || ver.Patch() != 0 {
				return errors.ExpectedFirstVersionInSeries(kv)
			}
		} else {
			// If the supplied version already exists, return a precondition
			// failed unless there was a force override option.
			if versions.Contains(*ver) {
				return errors.ExpectedNotToExist(kv)
			}
		}

		qs := `
INSERT INTO metas (
  system
, kind
, version
, schema
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
			systemRec.RowID,
			kindRec.RowID,
			kv.VersionString(),
			schemaJSON,
			createdOn,
			createdBy,
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.ExpectedNotToExist(kv)
				}
			}
			return errors.Internal(
				"failed inserting metas record",
				errors.WithWrap(err),
			)
		}
		return nil
	}
	return s.dbExec(ctx, fn)
}
