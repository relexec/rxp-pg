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
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/system"
)

// dbReadByRowID performs a SELECT query to return the stored system record
// having the supplied internal DB RowID.
func (s *Store) dbReadByRowID(
	ctx context.Context,
	rowID int64,
) (*Record, error) {
	out := Record{
		RowID: rowID,
	}
	fn := func(tx pgx.Tx) error {
		var uuid string
		var tag string
		qs := "SELECT uuid, tag FROM systems WHERE id = $1"
		err := tx.QueryRow(ctx, qs, rowID).Scan(&uuid, &tag)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading systems record",
				errors.WithWrap(err),
			)
		}
		out.System = system.New(
			system.WithUUID(uuid),
			system.WithTag(tag),
		)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByUUID performs a SELECT query to return the stored system record
// having the supplied UUID.
func (s *Store) dbReadByUUID(
	ctx context.Context,
	uuid string,
) (*Record, error) {
	out := Record{
		System: system.New(
			system.WithUUID(uuid),
		),
	}
	fn := func(tx pgx.Tx) error {
		var tag sql.NullString
		qs := "SELECT id, tag FROM systems WHERE uuid = $1"
		err := tx.QueryRow(ctx, qs, uuid).Scan(&out.RowID, &tag)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading systems record",
				errors.WithWrap(err),
			)
		}
		if tag.Valid {
			out.System.SetTag(tag.String)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbInsert atomically writes the supplied System to persistent storage.
func (s *Store) dbInsert(
	ctx context.Context,
	sys system.System,
) error {
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	var tag *string
	uuid := sys.UUID()
	sysTag := sys.Tag()
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
					return errors.DuplicateKey("system", "uuid", sys.UUID())
				}
			}
		}
		return err
	}
	if err := s.Exec(ctx, fn); err != nil {
		return errors.Internal(
			"failed inserting systems record",
			errors.WithWrap(err),
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
	expr query.Expression,
	opts query.Options,
) ([]*Record, error) {
	qargs := []any{}
	wheres := []string{}

	switch expr := expr.(type) {
	case query.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case system.UUIDPredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("s.uuid = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			case query.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("s.uuid = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		default:
			return nil, errors.UnsupportedPredicate(pred)
		}
	default:
		return nil, errors.UnsupportedExpression(expr)
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
			return errors.Internal(
				"failed reading system records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[systemRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting system records",
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
		sys := system.New(
			system.WithUUID(rec.UUID),
			system.WithTag(rec.Tag),
		)
		out = append(out, &Record{
			RowID:  rec.ID,
			System: sys,
		})
	}

	return out, nil
}
