package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
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
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"
	"github.com/relexec/rxp/system"
	"github.com/relexec/rxp/version"

	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

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
	if err := s.Exec(ctx, fn); err != nil {
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
	if err := s.Exec(ctx, fn); err != nil {
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
	if err := s.Exec(ctx, fn); err != nil {
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
	return s.Exec(ctx, fn)
}

type kindversionRecord struct {
	SystemID int64          `db:"system_id"`
	KindID   int64          `db:"kind_id"`
	ID       int64          `db:"kindversion_id"`
	Version  string         `db:"kindversion_version"`
	Schema   sql.NullString `db:"kindversion_schema"`
}

// dbReadByExpression queries zero or more KindVersions that match the given
// pre-validated expression and options.
func (s *Store) dbReadByExpression(
	ctx context.Context,
	expr expression.Expression,
	opts query.Options,
) ([]*Record, error) {
	qargs := []any{}
	wheres := []string{}

	switch expr := expr.(type) {
	case expression.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case kindversion.NamePredicate:
			op := pred.Op
			switch op {
			case expression.PredicateOperatorEqual:
				kvName := pred.Value.(api.KindVersionName)
				kindName := kvName.Kind()
				kindRec, err := s.kindStore.ReadByName(
					ctx, s.hostSystemRecord.System, kindName,
				)
				if err != nil {
					// If we're looking up kindversions by a non-existent kind,
					// just return am empty result since there's clearly not
					// going to be any matching kindversion records.
					if err == errors.ErrNotFound {
						return nil, nil
					}
					return nil, err
				}
				verStr := kvName.VersionString()
				wheres = append(wheres, fmt.Sprintf("(kv.kind = $%d AND kv.version = $%d)", len(qargs)+1, len(qargs)+2))
				qargs = append(qargs, kindRec.RowID)
				qargs = append(qargs, verStr)
			case expression.PredicateOperatorIn:
				ors := []string{}
				kvNames := pred.Value.([]api.KindVersionName)
				for _, kvName := range kvNames {
					kindName := kvName.Kind()
					kindRec, err := s.kindStore.ReadByName(
						ctx, s.hostSystemRecord.System, kindName,
					)
					if err != nil {
						if err == errors.ErrNotFound {
							continue
						}
						return nil, err
					}
					verStr := kvName.VersionString()
					ors = append(ors, fmt.Sprintf("(kv.kind = $%d AND kv.version = $%d)", len(qargs)+1, len(qargs)+2))
					qargs = append(qargs, kindRec.RowID)
					qargs = append(qargs, verStr)
				}
				wheres = append(wheres, "("+strings.Join(ors, " OR ")+")")
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		case system.UUIDPredicate:
			op := pred.Op
			switch op {
			case expression.PredicateOperatorEqual:
				sysUUID := pred.Value.(string)
				sysRec, err := s.systemStore.ReadByUUID(ctx, sysUUID)
				if err != nil {
					// If we're looking up kindversions by a non-existent system,
					// just return am empty result since there's clearly not
					// going to be any matching kindversion records.
					if err == errors.ErrNotFound {
						return nil, nil
					}
					return nil, err
				}
				wheres = append(wheres, fmt.Sprintf("kv.system = $%d", len(qargs)+1))
				qargs = append(qargs, sysRec.RowID)
			case expression.PredicateOperatorIn:
				sysRowIDs := []int64{}
				sysUUIDs := pred.Value.([]string)
				for _, sysUUID := range sysUUIDs {
					sysRec, err := s.systemStore.ReadByUUID(ctx, sysUUID)
					if err != nil {
						if err == errors.ErrNotFound {
							continue
						}
						return nil, err
					}
					sysRowIDs = append(sysRowIDs, sysRec.RowID)
				}
				if len(sysRowIDs) == 0 {
					// If we're looking up kindversions by a non-existent system,
					// just return am empty result since there's clearly not
					// going to be any matching kindversion records.
					return nil, nil
				}
				wheres = append(wheres, fmt.Sprintf("kv.system = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, sysRowIDs)
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		default:
			return nil, errors.UnsupportedPredicate(pred)
		}
	default:
		return nil, errors.UnsupportedExpression(expr)
	}

	var recs []kindversionRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  kv.system AS system_id
, kv.kind AS kind_id
, kv.id AS kindversion_id
, kv.version AS kindversion_version
, kv.schema AS kindversion_schema
FROM kindversions AS kv
`
		if len(wheres) > 0 {
			qs += "\nWHERE " + strings.Join(wheres, " AND ")
		}
		qs += fmt.Sprintf("\nORDER BY kv.kind ASC, kv.version ASC LIMIT %d", opts.Limit())
		rows, err := tx.Query(ctx, qs, qargs...)
		if err != nil {
			return errors.Internal(
				"failed reading kindversion records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[kindversionRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting kindversion records",
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
		sysRec, err := s.systemStore.ReadByRowID(ctx, rec.SystemID)
		if err != nil {
			return nil, errors.Internal(
				"failed reading system record by rowid",
				errors.WithWrap(err),
			)
		}
		kindRec, err := s.kindStore.ReadByRowID(ctx, rec.KindID)
		if err != nil {
			return nil, errors.Internal(
				"failed reading kind record by rowid",
				errors.WithWrap(err),
			)
		}
		var schema schema.Schema
		if rec.Schema.Valid {
			err = json.Unmarshal([]byte(rec.Schema.String), &schema)
			if err != nil {
				return nil, errors.Internal(
					"failed unmarshaling kindversion schema",
					errors.WithWrap(err),
				)
			}
		}
		sv, err := semver.NewVersion(rec.Version)
		if err != nil {
			return nil, errors.Internal(
				"failed parsing semver for kindversion",
				errors.WithWrap(err),
			)
		}
		kv := kindversion.New(
			kindversion.WithSystem(sysRec.System),
			kindversion.WithKind(kindRec.Kind),
			kindversion.WithVersion(*sv),
			kindversion.WithSchema(&schema),
		)
		out = append(out, &Record{
			RowID:       rec.ID,
			KindVersion: kv,
		})
	}

	return out, nil
}
