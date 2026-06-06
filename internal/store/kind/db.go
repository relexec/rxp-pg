package store

import (
	"context"
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
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"

	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// dbReadByRowID performs a SELECT query to return the stored kind record
// having the supplied internal DB RowID.
func (s *Store) dbReadByRowID(
	ctx context.Context,
	rowID int64,
) (*Record, error) {
	out := Record{
		RowID: rowID,
	}
	fn := func(tx pgx.Tx) error {
		var systemRowID int64
		var uuid string
		var name api.KindName
		var scope api.Scope
		qs := "SELECT system, uuid, name, scope FROM kinds WHERE id = $1"
		err := tx.QueryRow(
			ctx, qs, rowID,
		).Scan(&systemRowID, &uuid, &name, &scope)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading kinds record",
				errors.WithWrap(err),
			)
		}
		systemRec, err := s.systemStore.ReadByRowID(ctx, systemRowID)
		if err != nil {
			return errors.Internal(
				"failed reading system record for kind",
				errors.WithWrap(err),
			)
		}
		out.Kind = kind.New(
			kind.WithSystem(systemRec.System),
			kind.WithUUID(uuid),
			kind.WithName(name),
			kind.WithScope(scope),
		)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByUUID performs a SELECT query to return the stored kind record
// having the supplied UUID.
func (s *Store) dbReadByUUID(
	ctx context.Context,
	uuid string,
) (*Record, error) {
	out := Record{
		Kind: kind.New(
			kind.WithUUID(uuid),
		),
	}
	fn := func(tx pgx.Tx) error {
		var name api.KindName
		var scope api.Scope
		qs := "SELECT id, name, scope FROM kinds WHERE uuid = $1"
		err := tx.QueryRow(ctx, qs, uuid).Scan(&out.RowID, &name, &scope)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading kinds record",
				errors.WithWrap(err),
			)
		}
		out.Kind.SetName(name)
		out.Kind.SetScope(scope)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByName performs a SELECT query to return the stored kind record
// having the supplied Name.
func (s *Store) dbReadByName(
	ctx context.Context,
	systemRec *storesystem.Record,
	name api.KindName,
) (*Record, error) {
	out := Record{
		Kind: kind.New(
			kind.WithSystem(systemRec.System),
			kind.WithName(name),
		),
	}
	fn := func(tx pgx.Tx) error {
		var uuid string
		var scope api.Scope
		qs := "SELECT id, uuid, scope FROM kinds WHERE system = $1 AND name = $2"
		err := tx.QueryRow(
			ctx, qs, systemRec.RowID, name,
		).Scan(&out.RowID, &uuid, &scope)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading kinds record",
				errors.WithWrap(err),
			)
		}
		out.Kind.SetUUID(uuid)
		out.Kind.SetScope(scope)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbInsert atomically writes the supplied Kind to persistent storage.
func (s *Store) dbInsert(
	ctx context.Context,
	systemRec *storesystem.Record,
	kind *kind.Kind,
) error {
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
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
			ctx, qs, systemRec.RowID,
			kind.UUID(), kind.Name(), kind.Scope(),
			createdOn, createdBy,
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.DuplicateName("kind", kind.Name())
				}
			}
		}
		return err
	}
	if err := s.Exec(ctx, fn); err != nil {
		return errors.Internal(
			"failed inserting kinds record",
			errors.WithWrap(err),
		)
	}
	return nil
}

type kindRecord struct {
	SystemID int64        `db:"system_id"`
	ID       int64        `db:"kind_id"`
	UUID     string       `db:"kind_uuid"`
	Name     api.KindName `db:"kind_name"`
	Scope    api.Scope    `db:"kind_scope"`
}

// dbReadByExpression queries zero or more Kinds that match the given
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
		case expression.UUIDPredicate, expression.KindUUIDPredicate:
			op := pred.Operator()
			switch op {
			case expression.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("k.uuid = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value())
			case expression.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("k.uuid = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value())
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		case expression.KindNamePredicate:
			op := pred.Operator()
			switch op {
			case expression.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("k.name = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value())
			case expression.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("k.name = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value())
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		case expression.SystemUUIDPredicate:
			op := pred.Operator()
			switch op {
			case expression.PredicateOperatorEqual:
				sysUUID := pred.Value().(string)
				sysRec, err := s.systemStore.ReadByUUID(ctx, sysUUID)
				if err != nil {
					// If we're looking up kinds by a non-existent system,
					// just return am empty result since there's clearly not
					// going to be any matching kind records.
					if err == errors.ErrNotFound {
						return nil, nil
					}
					return nil, err
				}
				wheres = append(wheres, fmt.Sprintf("k.system = $%d", len(qargs)+1))
				qargs = append(qargs, sysRec.RowID)
			case expression.PredicateOperatorIn:
				sysRowIDs := []int64{}
				sysUUIDs := pred.Value().([]string)
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
					// If we're looking up kinds by a non-existent system,
					// just return am empty result since there's clearly not
					// going to be any matching kind records.
					return nil, nil
				}
				wheres = append(wheres, fmt.Sprintf("k.system = ANY ($%d)", len(qargs)+1))
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
			return errors.Internal(
				"failed reading kind records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[kindRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting kind records",
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
		k := kind.New(
			kind.WithUUID(rec.UUID),
			kind.WithName(rec.Name),
			kind.WithSystem(sysRec.System),
			kind.WithScope(rec.Scope),
		)
		out = append(out, &Record{
			RowID: rec.ID,
			Kind:  k,
		})
	}

	return out, nil
}
