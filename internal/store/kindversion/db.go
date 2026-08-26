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
	"github.com/relexec/pkg/version"
	apicore "github.com/relexec/rxp/api/core"
	apierrors "github.com/relexec/rxp/api/errors"
	apikind "github.com/relexec/rxp/api/kind"
	apikindversion "github.com/relexec/rxp/api/kindversion"
	"github.com/relexec/rxp/api/kindversion/schema"
	apisystem "github.com/relexec/rxp/api/system"
	"github.com/relexec/rxp/query"
)

// dbReadByRowID performs a SELECT query to return the stored kindversion
// record having the supplied internal DB RowID.
func (s *Store) dbReadByRowID(
	ctx context.Context,
	sysRec *apisystem.System,
	kindRec *apikind.Kind,
	rowID int64,
) (*apikindversion.KindVersion, error) {
	var verStr string
	var schemaBytes sql.NullString
	var schema schema.Schema
	out := &apikindversion.KindVersion{
		System: sysRec,
		Kind:   *kindRec,
	}
	out.SetSystemInternalID(rowID)
	fn := func(tx pgx.Tx) error {
		qs := "SELECT version, schema FROM kindversions WHERE id = $1"
		err := tx.QueryRow(
			ctx, qs, rowID,
		).Scan(
			&verStr, &schemaBytes,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return apierrors.ErrNotFound
			}
			return apierrors.Internal(
				"failed reading kindversions record",
				apierrors.WithWrap(err),
			)
		}
		if schemaBytes.Valid {
			err = json.Unmarshal([]byte(schemaBytes.String), &schema)
			if err != nil {
				return apierrors.Internal(
					"failed unmarshaling kindversion schema",
					apierrors.WithWrap(err),
				)
			}
		}
		sv, err := semver.NewVersion(verStr)
		if err != nil {
			return apierrors.Internal(
				"failed parsing semver for kindversion",
				apierrors.WithWrap(err),
			)
		}
		out.Version = *sv
		out.Schema = &schema
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return out, nil
}

// dbReadByName performs a SELECT query to return the stored kindversion record
// having the supplied KindVersion.
func (s *Store) dbReadByName(
	ctx context.Context,
	sysRec *apisystem.System,
	kindRec *apikind.Kind,
	kv apikindversion.Name,
) (*apikindversion.KindVersion, error) {
	sysRowID := sysRec.SystemInternalIDInt64()
	kindRowID := kindRec.SystemInternalIDInt64()
	sv, _ := kv.Version()
	verStr := kv.VersionString()
	var rowID int64
	var schemaBytes sql.NullString
	var schema schema.Schema
	out := &apikindversion.KindVersion{
		System: sysRec,
		Kind:   *kindRec,
	}
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
			sysRowID, kindRowID, verStr,
		).Scan(
			&rowID, &schemaBytes,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return apierrors.ErrNotFound
			}
			return apierrors.Internal(
				"failed reading kindversions record",
				apierrors.WithWrap(err),
			)
		}
		if schemaBytes.Valid {
			err = json.Unmarshal([]byte(schemaBytes.String), &schema)
			if err != nil {
				return apierrors.Internal(
					"failed unmarshaling kindversion schema",
					apierrors.WithWrap(err),
				)
			}
		}
		out.Version = *sv
		out.Schema = &schema
		out.SetSystemInternalID(rowID)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return out, nil
}

// dbVersionsForKind returns a version.Set representing all the semantic
// versions known for the supplied Kind.
func (s *Store) dbVersionsForKind(
	ctx context.Context,
	sysRec *apisystem.System,
	kindRec *apikind.Kind,
) (version.Set, error) {
	sysRowID := sysRec.SystemInternalIDInt64()
	kindRowID := kindRec.SystemInternalIDInt64()
	var versionStrs []string
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT version
FROM kindversions
WHERE system = $1
AND kind = $2
`
		rows, err := tx.Query(ctx, qs, sysRowID, kindRowID)
		if err != nil {
			return apierrors.Internal(
				"failed reading kindversion records",
				apierrors.WithWrap(err),
			)
		}
		defer rows.Close()
		versionStrs, err = pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return apierrors.Internal(
				"failed collecting kindversion versions",
				apierrors.WithWrap(err),
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
			return nil, apierrors.Internal(
				"failed parsing semver",
				apierrors.WithWrap(err),
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
	sysRec *apisystem.System,
	kindRec *apikind.Kind,
	kv apikindversion.KindVersion,
) error {
	sysRowID := sysRec.SystemInternalIDInt64()
	kindRowID := kindRec.SystemInternalIDInt64()
	name := kv.Name()
	ver, _ := name.Version()
	createdOn := time.Now().UnixNano()
	caller := apicore.CallerFromContext(ctx)
	createdBy := caller.Identity
	schemaJSON, err := kv.SchemaJSON()
	if err != nil {
		return err
	}
	fn := func(tx pgx.Tx) error {
		versions, err := s.dbVersionsForKind(ctx, sysRec, kindRec)
		if err != nil {
			return err
		}
		if len(versions) == 0 {
			// ensure we were given the first version in the version series OR
			// there was a force override option.
			if ver.Minor() != 0 || ver.Patch() != 0 {
				return apierrors.ExpectedFirstVersionInSeries(kv.Name())
			}
		} else {
			// If the supplied version already exists, return a precondition
			// failed unless there was a force override option.
			if versions.Contains(*ver) {
				return apierrors.ExpectedNotToExist(kv.Name())
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
			sysRowID,
			kindRowID,
			name.VersionString(),
			schemaJSON,
			createdOn,
			createdBy,
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return apierrors.ExpectedNotToExist(kv.Name())
				}
			}
			return apierrors.Internal(
				"failed inserting kindversions record",
				apierrors.WithWrap(err),
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
	expr query.Expression,
	opts query.Options,
) ([]*apikindversion.KindVersion, error) {
	qargs := []any{}
	wheres := []string{}

	switch expr := expr.(type) {
	case query.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case apikindversion.NamePredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				kvName := pred.Value.(apikindversion.Name)
				kindName := kvName.Kind()
				kindRec, err := s.kindStore.ReadByName(
					ctx, &s.hostSystemRecord, kindName,
				)
				if err != nil {
					// If we're looking up kindversions by a non-existent kind,
					// just return am empty result since there's clearly not
					// going to be any matching kindversion records.
					if err == apierrors.ErrNotFound {
						return nil, nil
					}
					return nil, err
				}
				kindRowID := kindRec.SystemInternalIDInt64()
				verStr := kvName.VersionString()
				wheres = append(wheres, fmt.Sprintf("(kv.kind = $%d AND kv.version = $%d)", len(qargs)+1, len(qargs)+2))
				qargs = append(qargs, kindRowID)
				qargs = append(qargs, verStr)
			case query.PredicateOperatorIn:
				ors := []string{}
				kvNames := pred.Value.([]apikindversion.Name)
				for _, kvName := range kvNames {
					kindName := kvName.Kind()
					kindRec, err := s.kindStore.ReadByName(
						ctx, &s.hostSystemRecord, kindName,
					)
					if err != nil {
						if err == apierrors.ErrNotFound {
							continue
						}
						return nil, err
					}
					kindRowID := kindRec.SystemInternalIDInt64()
					verStr := kvName.VersionString()
					ors = append(ors, fmt.Sprintf("(kv.kind = $%d AND kv.version = $%d)", len(qargs)+1, len(qargs)+2))
					qargs = append(qargs, kindRowID)
					qargs = append(qargs, verStr)
				}
				wheres = append(wheres, "("+strings.Join(ors, " OR ")+")")
			default:
				return nil, apierrors.UnsupportedPredicateOperator(op)
			}
		case apisystem.UUIDPredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				sysUUID := pred.Value.(string)
				sysRec, err := s.systemStore.ReadByUUID(ctx, sysUUID)
				if err != nil {
					// If we're looking up kindversions by a non-existent system,
					// just return am empty result since there's clearly not
					// going to be any matching kindversion records.
					if err == apierrors.ErrNotFound {
						return nil, nil
					}
					return nil, err
				}
				sysRowID := sysRec.SystemInternalIDInt64()
				wheres = append(wheres, fmt.Sprintf("kv.system = $%d", len(qargs)+1))
				qargs = append(qargs, sysRowID)
			case query.PredicateOperatorIn:
				sysRowIDs := []int64{}
				sysUUIDs := pred.Value.([]string)
				for _, sysUUID := range sysUUIDs {
					sysRec, err := s.systemStore.ReadByUUID(ctx, sysUUID)
					if err != nil {
						if err == apierrors.ErrNotFound {
							continue
						}
						return nil, err
					}
					sysRowID := sysRec.SystemInternalIDInt64()
					sysRowIDs = append(sysRowIDs, sysRowID)
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
				return nil, apierrors.UnsupportedPredicateOperator(op)
			}
		default:
			return nil, apierrors.UnsupportedPredicate(pred)
		}
	default:
		return nil, apierrors.UnsupportedExpression(expr)
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
			return apierrors.Internal(
				"failed reading kindversion records",
				apierrors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[kindversionRecord])
		if err != nil {
			return apierrors.Internal(
				"failed collecting kindversion records",
				apierrors.WithWrap(err),
			)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}

	out := make([]*apikindversion.KindVersion, 0, len(recs))
	for _, rec := range recs {
		sysRec, err := s.systemStore.ReadByRowID(ctx, rec.SystemID)
		if err != nil {
			return nil, apierrors.Internal(
				"failed reading system record by rowid",
				apierrors.WithWrap(err),
			)
		}
		kindRec, err := s.kindStore.ReadByRowID(ctx, rec.KindID)
		if err != nil {
			return nil, apierrors.Internal(
				"failed reading kind record by rowid",
				apierrors.WithWrap(err),
			)
		}
		var schema schema.Schema
		if rec.Schema.Valid {
			err = json.Unmarshal([]byte(rec.Schema.String), &schema)
			if err != nil {
				return nil, apierrors.Internal(
					"failed unmarshaling kindversion schema",
					apierrors.WithWrap(err),
				)
			}
		}
		sv, err := semver.NewVersion(rec.Version)
		if err != nil {
			return nil, apierrors.Internal(
				"failed parsing semver for kindversion",
				apierrors.WithWrap(err),
			)
		}
		kv := &apikindversion.KindVersion{
			System:  sysRec,
			Kind:    *kindRec,
			Version: *sv,
			Schema:  &schema,
		}
		kv.SetSystemInternalID(rec.ID)
		out = append(out, kv)
	}

	return out, nil
}
