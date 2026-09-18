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
	"github.com/relexec/rxp"
	rxperrors "github.com/relexec/rxp/errors"
	rxpquery "github.com/relexec/rxp/query"
	rxpsystem "github.com/relexec/rxp/system"
)

// dbReadByRowID performs a SELECT query to return the stored system record
// having the supplied internal DB RowID.
func (s *Store) dbReadByRowID(
	ctx context.Context,
	rowID int64,
) (*rxp.System, error) {
	out := &rxp.System{}
	out.SetSystemInternalID(rowID)
	fn := func(tx pgx.Tx) error {
		var uuid string
		var tag string
		qs := "SELECT uuid, tag FROM systems WHERE id = $1"
		err := tx.QueryRow(ctx, qs, rowID).Scan(&uuid, &tag)
		if err != nil {
			if err == pgx.ErrNoRows {
				return rxperrors.ErrNotFound
			}
			return rxperrors.Internal(
				"failed reading systems record by rowid",
				rxperrors.WithWrap(err),
			)
		}
		out.UUID = uuid
		out.Tag = tag
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return out, nil
}

// dbReadByUUID performs a SELECT query to return the stored system record
// having the supplied UUID.
func (s *Store) dbReadByUUID(
	ctx context.Context,
	uuid string,
) (*rxp.System, error) {
	out := &rxp.System{UUID: uuid}
	fn := func(tx pgx.Tx) error {
		var rowID int64
		var tag sql.NullString
		qs := "SELECT id, tag FROM systems WHERE uuid = $1"
		err := tx.QueryRow(ctx, qs, uuid).Scan(&rowID, &tag)
		if err != nil {
			if err == pgx.ErrNoRows {
				return rxperrors.ErrNotFound
			}
			return rxperrors.Internal(
				"failed reading systems record by uuid",
				rxperrors.WithWrap(err),
			)
		}
		out.SetSystemInternalID(rowID)
		if tag.Valid {
			out.Tag = tag.String
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return out, nil
}

// dbInsert atomically writes the supplied System to persistent storage.
func (s *Store) dbInsert(
	ctx context.Context,
	sys rxp.System,
) error {
	createdOn := time.Now().UnixNano()
	caller := rxp.CallerFromContext(ctx)
	createdBy := caller.Identity
	var tag *string
	uuid := sys.UUID
	sysTag := sys.Tag
	if sysTag != "" {
		tag = &sysTag
	}
	fn := func(tx pgx.Tx) error {
		qs := `
INSERT INTO systems (
  uuid
, tag
, last_modified_on
, last_modified_by
) VALUES (
  $1
, $2
, $3
, $4
)`
		_, err := tx.Exec(ctx, qs, uuid, tag, createdOn, createdBy)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return rxperrors.DuplicateKey("system", "uuid", uuid)
				}
			}
		}
		return err
	}
	if err := s.Exec(ctx, fn); err != nil {
		return rxperrors.Internal(
			"failed inserting systems record",
			rxperrors.WithWrap(err),
		)
	}
	return nil
}

type systemRecord struct {
	ID   int64  `db:"system_id"`
	UUID string `db:"system_uuid"`
	Tag  string `db:"system_tag"`
}

// dbReadByExpression queries zero or more Systems that match the given
// pre-validated expression and options.
func (s *Store) dbReadByExpression(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) ([]*rxp.System, error) {
	qargs := []any{}
	wheres := []string{}

	switch expr := expr.(type) {
	case rxpquery.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case rxpsystem.UUIDPredicate:
			op := pred.Op
			switch op {
			case rxpquery.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("s.uuid = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			case rxpquery.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("s.uuid = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, rxperrors.UnsupportedPredicateOperator(op)
			}
		default:
			return nil, rxperrors.UnsupportedPredicate(pred)
		}
	default:
		return nil, rxperrors.UnsupportedExpression(expr)
	}

	var recs []systemRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  s.id AS system_id
, s.uuid AS system_uuid
, s.tag AS system_tag
FROM systems AS s
`
		if len(wheres) > 0 {
			qs += "\nWHERE " + strings.Join(wheres, " AND ")
		}
		qs += fmt.Sprintf("\nORDER BY s.uuid ASC LIMIT %d", opts.Limit())
		rows, err := tx.Query(ctx, qs, qargs...)
		if err != nil {
			return rxperrors.Internal(
				"failed reading system records",
				rxperrors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[systemRecord])
		if err != nil {
			return rxperrors.Internal(
				"failed collecting system records",
				rxperrors.WithWrap(err),
			)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}

	out := make([]*rxp.System, 0, len(recs))
	for _, rec := range recs {
		sys := &rxp.System{
			UUID: rec.UUID,
			Tag:  rec.Tag,
		}
		sys.SetSystemInternalID(rec.ID)
		out = append(out, sys)
	}

	return out, nil
}
