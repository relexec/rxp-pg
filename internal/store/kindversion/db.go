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
	"github.com/relexec/rxp/api"
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind/kindversion"
	"github.com/relexec/rxp/kind/kindversion/schema"
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

// dbReadByRowID performs a SELECT query to return the stored kindversion
// record having the supplied internal DB RowID.
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
		qs := "SELECT system, kind, version, schema FROM kindversions WHERE id = $1"
		err := tx.QueryRow(
			ctx, qs, rowID,
		).Scan(
			&systemRowID, &kindRowID, &verStr, &schemaBytes,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading kindversions record",
				errors.WithWrap(err),
			)
		}
		systemRec, err := s.systemStore.ReadByRowID(ctx, systemRowID)
		if err != nil {
			return errors.Internal(
				"failed reading system record for kindversion",
				errors.WithWrap(err),
			)
		}
		kindRec, err := s.kindStore.ReadByRowID(ctx, kindRowID)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading kind record for kindversion",
				errors.WithWrap(err),
			)
		}
		if schemaBytes.Valid {
			err = json.Unmarshal([]byte(schemaBytes.String), &schema)
			if err != nil {
				return errors.Internal(
					"failed unmarshaling kindversion schema",
					errors.WithWrap(err),
				)
			}
		}
		sv, err := semver.NewVersion(verStr)
		if err != nil {
			return errors.Internal(
				"failed parsing semver for kindversion",
				errors.WithWrap(err),
			)
		}
		out.KindVersion = kindversion.New(
			kindversion.WithSystem(systemRec.System),
			kindversion.WithKind(kindRec.Kind),
			kindversion.WithVersion(*sv),
			kindversion.WithSchema(&schema),
		)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByName performs a SELECT query to return the stored kindversion record
// having the supplied KindVersion.
func (s *Store) dbReadByName(
	ctx context.Context,
	systemRec *storesystem.Record,
	kindRec *storekind.Record,
	kv api.KindVersionName,
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
FROM kindversions
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
				"failed reading kindversions record",
				errors.WithWrap(err),
			)
		}
		if schemaBytes.Valid {
			err = json.Unmarshal([]byte(schemaBytes.String), &schema)
			if err != nil {
				return errors.Internal(
					"failed unmarshaling kindversion schema",
					errors.WithWrap(err),
				)
			}
		}
		out.KindVersion = kindversion.New(
			kindversion.WithSystem(systemRec.System),
			kindversion.WithKind(kindRec.Kind),
			kindversion.WithVersion(*sv),
			kindversion.WithSchema(&schema),
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
		qs := "SELECT version FROM kindversions WHERE system = $1 AND kind = $2"
		rows, err := tx.Query(ctx, qs, systemRec.RowID, kindRec.RowID)
		if err != nil {
			return errors.Internal(
				"failed reading kindversion records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		versionStrs, err = pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return errors.Internal(
				"failed collecting kindversion versions",
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

// dbInsert atomically writes the supplied KindVersion to persistent storage.
func (s *Store) dbInsert(
	ctx context.Context,
	systemRec *storesystem.Record,
	kindRec *storekind.Record,
	kv *kindversion.KindVersion,
) error {
	name := kv.Name()
	ver, _ := name.Version()
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	schemaJSON, err := kv.SchemaJSON()
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
				return errors.ExpectedFirstVersionInSeries(kv.Name())
			}
		} else {
			// If the supplied version already exists, return a precondition
			// failed unless there was a force override option.
			if versions.Contains(*ver) {
				return errors.ExpectedNotToExist(kv.Name())
			}
		}

		qs := `
INSERT INTO kindversions (
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
			name.VersionString(),
			schemaJSON,
			createdOn,
			createdBy,
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.ExpectedNotToExist(kv.Name())
				}
			}
			return errors.Internal(
				"failed inserting kindversions record",
				errors.WithWrap(err),
			)
		}
		return nil
	}
	return s.dbExec(ctx, fn)
}
