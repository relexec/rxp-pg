package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	apicore "github.com/relexec/rxp/api/core"
	apierrors "github.com/relexec/rxp/api/errors"
	rxpkind "github.com/relexec/rxp/api/kind"
	rxpquery "github.com/relexec/rxp/api/query"
	rxpsystem "github.com/relexec/rxp/api/system"
)

// dbReadByRowID performs a SELECT query to return the stored kind record
// having the supplied internal DB RowID.
func (s *Store) dbReadByRowID(
	ctx context.Context,
	rowID int64,
) (*rxpkind.Kind, error) {
	out := &rxpkind.Kind{}
	out.SetSystemInternalID(rowID)
	fn := func(tx pgx.Tx) error {
		var systemRowID int64
		var uuid string
		var name rxpkind.Name
		var scope apicore.Scope
		qs := "SELECT system, uuid, name, scope FROM kinds WHERE id = $1"
		err := tx.QueryRow(
			ctx, qs, rowID,
		).Scan(&systemRowID, &uuid, &name, &scope)
		if err != nil {
			if err == pgx.ErrNoRows {
				return apierrors.ErrNotFound
			}
			return apierrors.Internal(
				"failed reading kinds record by rowid",
				apierrors.WithWrap(err),
			)
		}
		sysRec, err := s.systemStore.ReadByRowID(ctx, systemRowID)
		if err != nil {
			return apierrors.Internal(
				"failed reading system record for kind",
				apierrors.WithWrap(err),
			)
		}
		out.System = sysRec
		out.UUID = uuid
		out.Name = name
		out.Scope = scope
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return out, nil
}

// dbReadByUUID performs a SELECT query to return the stored kind record
// having the supplied UUID.
func (s *Store) dbReadByUUID(
	ctx context.Context,
	uuid string,
) (*rxpkind.Kind, error) {
	out := &rxpkind.Kind{
		UUID: uuid,
	}
	fn := func(tx pgx.Tx) error {
		var rowID int64
		var name rxpkind.Name
		var scope apicore.Scope
		qs := "SELECT id, name, scope FROM kinds WHERE uuid = $1"
		err := tx.QueryRow(ctx, qs, uuid).Scan(&rowID, &name, &scope)
		if err != nil {
			if err == pgx.ErrNoRows {
				return apierrors.ErrNotFound
			}
			return apierrors.Internal(
				"failed reading kinds record by uuid",
				apierrors.WithWrap(err),
			)
		}
		out.SetSystemInternalID(rowID)
		out.Name = name
		out.Scope = scope
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return out, nil
}

// dbReadByName performs a SELECT query to return the stored kind record
// having the supplied Name.
func (s *Store) dbReadByName(
	ctx context.Context,
	sysRec *rxpsystem.System,
	name rxpkind.Name,
) (*rxpkind.Kind, error) {
	out := &rxpkind.Kind{
		System: sysRec,
		Name:   name,
	}
	sysRowID := sysRec.SystemInternalIDInt64()
	fn := func(tx pgx.Tx) error {
		var rowID int64
		var uuid string
		var scope apicore.Scope
		qs := `
SELECT id, uuid, scope
FROM kinds
WHERE system = $1
AND name = $2
`
		err := tx.QueryRow(
			ctx, qs, sysRowID, name,
		).Scan(&rowID, &uuid, &scope)
		if err != nil {
			if err == pgx.ErrNoRows {
				return apierrors.ErrNotFound
			}
			return apierrors.Internal(
				"failed reading kinds record by name",
				apierrors.WithWrap(err),
			)
		}
		out.SetSystemInternalID(rowID)
		out.UUID = uuid
		out.Scope = scope
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return out, nil
}

// dbInsert atomically writes the supplied Kind to persistent storage.
func (s *Store) dbInsert(
	ctx context.Context,
	sysRec *rxpsystem.System,
	kind rxpkind.Kind,
) error {
	sysRowID := sysRec.SystemInternalIDInt64()
	createdOn := time.Now().UnixNano()
	caller := apicore.CallerFromContext(ctx)
	createdBy := caller.Identity
	fn := func(tx pgx.Tx) error {
		qs := `
INSERT INTO kinds (
  system
, uuid
, name
, scope
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
		_, err := tx.Exec(
			ctx, qs, sysRowID,
			kind.UUID, kind.Name, kind.Scope,
			createdOn, createdBy,
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return apierrors.DuplicateName("kind", kind.Name)
				}
			}
		}
		return err
	}
	if err := s.Exec(ctx, fn); err != nil {
		return apierrors.Internal(
			"failed inserting kinds record",
			apierrors.WithWrap(err),
		)
	}
	return nil
}

type kindRecord struct {
	SystemID int64         `db:"system_id"`
	ID       int64         `db:"kind_id"`
	UUID     string        `db:"kind_uuid"`
	Name     rxpkind.Name  `db:"kind_name"`
	Scope    apicore.Scope `db:"kind_scope"`
}

// dbReadByExpression queries zero or more Kinds that match the given
// pre-validated expression and options.
func (s *Store) dbReadByExpression(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) ([]*rxpkind.Kind, error) {
	qargs := []any{}
	wheres := []string{}

	switch expr := expr.(type) {
	case rxpquery.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case rxpkind.UUIDPredicate:
			op := pred.Op
			switch op {
			case rxpquery.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("k.uuid = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			case rxpquery.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("k.uuid = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, apierrors.UnsupportedPredicateOperator(op)
			}
		case rxpkind.NamePredicate:
			op := pred.Op
			switch op {
			case rxpquery.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("k.name = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			case rxpquery.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("k.name = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, apierrors.UnsupportedPredicateOperator(op)
			}
		case rxpsystem.UUIDPredicate:
			op := pred.Op
			switch op {
			case rxpquery.PredicateOperatorEqual:
				sysUUID := pred.Value.(string)
				sysRec, err := s.systemStore.ReadByUUID(ctx, sysUUID)
				if err != nil {
					// If we're looking up kinds by a non-existent system,
					// just return am empty result since there's clearly not
					// going to be any matching kind records.
					if err == apierrors.ErrNotFound {
						return nil, nil
					}
					return nil, err
				}
				sysRowID := sysRec.SystemInternalIDInt64()
				wheres = append(wheres, fmt.Sprintf("k.system = $%d", len(qargs)+1))
				qargs = append(qargs, sysRowID)
			case rxpquery.PredicateOperatorIn:
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
					// If we're looking up kinds by a non-existent system,
					// just return am empty result since there's clearly not
					// going to be any matching kind records.
					return nil, nil
				}
				wheres = append(wheres, fmt.Sprintf("k.system = ANY ($%d)", len(qargs)+1))
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

	var recs []kindRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  k.system AS system_id
, k.id AS kind_id
, k.uuid AS kind_uuid
, k.name AS kind_name
, k.scope AS kind_scope
FROM kinds AS k
`
		if len(wheres) > 0 {
			qs += "\nWHERE " + strings.Join(wheres, " AND ")
		}
		qs += fmt.Sprintf("\nORDER BY k.uuid ASC LIMIT %d", opts.Limit())
		rows, err := tx.Query(ctx, qs, qargs...)
		if err != nil {
			return apierrors.Internal(
				"failed reading kind records",
				apierrors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[kindRecord])
		if err != nil {
			return apierrors.Internal(
				"failed collecting kind records",
				apierrors.WithWrap(err),
			)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}

	out := make([]*rxpkind.Kind, 0, len(recs))
	for _, rec := range recs {
		sysRec, err := s.systemStore.ReadByRowID(ctx, rec.SystemID)
		if err != nil {
			return nil, apierrors.Internal(
				"failed reading system record by rowid",
				apierrors.WithWrap(err),
			)
		}
		k := &rxpkind.Kind{
			UUID:   rec.UUID,
			Name:   rec.Name,
			System: sysRec,
			Scope:  rec.Scope,
		}
		k.SetSystemInternalID(rec.ID)

		out = append(out, k)
	}

	return out, nil
}
