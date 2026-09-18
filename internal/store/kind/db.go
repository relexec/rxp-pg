package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/relexec/rxp"
	rxperrors "github.com/relexec/rxp/errors"
	rxpkind "github.com/relexec/rxp/kind"
	rxpquery "github.com/relexec/rxp/query"
	rxpsystem "github.com/relexec/rxp/system"
)

// dbReadByRowID performs a SELECT query to return the stored kind record
// having the supplied internal DB RowID.
func (s *Store) dbReadByRowID(
	ctx context.Context,
	rowID int64,
) (*rxp.Kind, error) {
	out := &rxp.Kind{}
	out.SetSystemInternalID(rowID)
	fn := func(tx pgx.Tx) error {
		var systemRowID int64
		var uuid string
		var name rxp.KindName
		var scope rxp.Scope
		qs := "SELECT system, uuid, name, scope FROM kinds WHERE id = $1"
		err := tx.QueryRow(
			ctx, qs, rowID,
		).Scan(&systemRowID, &uuid, &name, &scope)
		if err != nil {
			if err == pgx.ErrNoRows {
				return rxperrors.ErrNotFound
			}
			return rxperrors.Internal(
				"failed reading kinds record by rowid",
				rxperrors.WithWrap(err),
			)
		}
		sysRec, err := s.systemStore.ReadByRowID(ctx, systemRowID)
		if err != nil {
			return rxperrors.Internal(
				"failed reading system record for kind",
				rxperrors.WithWrap(err),
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
) (*rxp.Kind, error) {
	out := &rxp.Kind{
		UUID: uuid,
	}
	fn := func(tx pgx.Tx) error {
		var rowID int64
		var name rxp.KindName
		var scope rxp.Scope
		qs := "SELECT id, name, scope FROM kinds WHERE uuid = $1"
		err := tx.QueryRow(ctx, qs, uuid).Scan(&rowID, &name, &scope)
		if err != nil {
			if err == pgx.ErrNoRows {
				return rxperrors.ErrNotFound
			}
			return rxperrors.Internal(
				"failed reading kinds record by uuid",
				rxperrors.WithWrap(err),
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
	sysRec *rxp.System,
	name rxp.KindName,
) (*rxp.Kind, error) {
	out := &rxp.Kind{
		System: sysRec,
		Name:   name,
	}
	sysRowID := sysRec.SystemInternalIDInt64()
	fn := func(tx pgx.Tx) error {
		var rowID int64
		var uuid string
		var scope rxp.Scope
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
				return rxperrors.ErrNotFound
			}
			return rxperrors.Internal(
				"failed reading kinds record by name",
				rxperrors.WithWrap(err),
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
	sysRec *rxp.System,
	kind rxp.Kind,
) error {
	sysRowID := sysRec.SystemInternalIDInt64()
	createdOn := time.Now().UnixNano()
	caller := rxp.CallerFromContext(ctx)
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
					return rxperrors.DuplicateName("kind", kind.Name)
				}
			}
		}
		return err
	}
	if err := s.Exec(ctx, fn); err != nil {
		return rxperrors.Internal(
			"failed inserting kinds record",
			rxperrors.WithWrap(err),
		)
	}
	return nil
}

type kindRecord struct {
	SystemID int64        `db:"system_id"`
	ID       int64        `db:"kind_id"`
	UUID     string       `db:"kind_uuid"`
	Name     rxp.KindName `db:"kind_name"`
	Scope    rxp.Scope    `db:"kind_scope"`
}

// dbReadByExpression queries zero or more Kinds that match the given
// pre-validated expression and options.
func (s *Store) dbReadByExpression(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) ([]*rxp.Kind, error) {
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
				return nil, rxperrors.UnsupportedPredicateOperator(op)
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
				return nil, rxperrors.UnsupportedPredicateOperator(op)
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
					if err == rxperrors.ErrNotFound {
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
						if err == rxperrors.ErrNotFound {
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
				return nil, rxperrors.UnsupportedPredicateOperator(op)
			}
		default:
			return nil, rxperrors.UnsupportedPredicate(pred)
		}
	default:
		return nil, rxperrors.UnsupportedExpression(expr)
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
			return rxperrors.Internal(
				"failed reading kind records",
				rxperrors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[kindRecord])
		if err != nil {
			return rxperrors.Internal(
				"failed collecting kind records",
				rxperrors.WithWrap(err),
			)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}

	out := make([]*rxp.Kind, 0, len(recs))
	for _, rec := range recs {
		sysRec, err := s.systemStore.ReadByRowID(ctx, rec.SystemID)
		if err != nil {
			return nil, rxperrors.Internal(
				"failed reading system record by rowid",
				rxperrors.WithWrap(err),
			)
		}
		k := &rxp.Kind{
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
