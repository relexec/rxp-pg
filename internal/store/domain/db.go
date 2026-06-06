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
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"

	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// dbReadByRowID performs a SELECT query to return the stored domain record
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
		var name api.DomainName
		var uuid string
		qs := "SELECT system, uuid, name FROM domains WHERE id = $1"
		err := tx.QueryRow(ctx, qs, rowID).Scan(&systemRowID, &uuid, &name)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domains record",
				errors.WithWrap(err),
			)
		}
		systemRec, err := s.systemStore.ReadByRowID(ctx, systemRowID)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading system record for domain",
				errors.WithWrap(err),
			)
		}
		out.Domain = domain.New(
			domain.WithSystem(systemRec.System),
			domain.WithUUID(uuid),
			domain.WithName(name),
		)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByUUID performs a SELECT query to return the stored domain record
// having the supplied UUID.
func (s *Store) dbReadByUUID(
	ctx context.Context,
	uuid string,
) (*Record, error) {
	out := Record{
		Domain: domain.New(
			domain.WithUUID(uuid),
		),
	}
	fn := func(tx pgx.Tx) error {
		var name api.DomainName
		qs := "SELECT id, name FROM domains WHERE uuid = $1"
		err := tx.QueryRow(ctx, qs, uuid).Scan(&out.RowID, &name)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domains record",
				errors.WithWrap(err),
			)
		}
		out.Domain.SetName(name)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByName performs a SELECT query to return the stored domain record
// having the supplied Name.
func (s *Store) dbReadByName(
	ctx context.Context,
	systemRec *storesystem.Record,
	name api.DomainName,
) (*Record, error) {
	out := Record{
		Domain: domain.New(
			domain.WithSystem(systemRec.System),
			domain.WithName(name),
		),
	}
	fn := func(tx pgx.Tx) error {
		var uuid string
		qs := "SELECT id, uuid FROM domains WHERE system = $1 AND name = $2"
		err := tx.QueryRow(ctx, qs, systemRec.RowID, name).Scan(&out.RowID, &uuid)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domains record",
				errors.WithWrap(err),
			)
		}
		out.Domain.SetUUID(uuid)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbInsert atomically writes the supplied Domain to persistent storage.
func (s *Store) dbInsert(
	ctx context.Context,
	systemRec *storesystem.Record,
	dom *domain.Domain,
) error {
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	uuid := dom.UUID()
	name := dom.Name()
	fn := func(tx pgx.Tx) error {
		qs := `
INSERT INTO domains (
  system
, uuid
, name
, last_modified_on
, last_modified_by
) VALUES (
  $1
, $2
, $3
, $4
, $5
)`
		_, err := tx.Exec(
			ctx, qs,
			systemRec.RowID,
			uuid,
			name,
			createdOn,
			createdBy,
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					conName := pgErr.ConstraintName
					if strings.Contains(conName, "uuid") {
						return errors.DuplicateKey("domain", "uuid", uuid)
					} else {
						return errors.DuplicateName("domain", name)
					}
				}
			}
		}
		return err
	}
	if err := s.Exec(ctx, fn); err != nil {
		return errors.Internal(
			"failed inserting domains record",
			errors.WithWrap(err),
		)
	}
	return nil
}

type domainRecord struct {
	SystemID int64          `db:"system_id"`
	ID       int64          `db:"domain_id"`
	UUID     string         `db:"domain_uuid"`
	Name     api.DomainName `db:"domain_name"`
}

// dbReadByExpression queries zero or more Domains that match the given
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
		case expression.UUIDPredicate, expression.DomainUUIDPredicate:
			op := pred.Operator()
			switch op {
			case expression.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("d.uuid = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value())
			case expression.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("d.uuid = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value())
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		case expression.DomainNamePredicate:
			op := pred.Operator()
			switch op {
			case expression.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("d.name = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value())
			case expression.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("d.name = ANY ($%d)", len(qargs)+1))
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
					// If we're looking up domains by a non-existent system,
					// just return am empty result since there's clearly not
					// going to be any matching domain records.
					if err == errors.ErrNotFound {
						return nil, nil
					}
					return nil, err
				}
				wheres = append(wheres, fmt.Sprintf("d.system = $%d", len(qargs)+1))
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
					// If we're looking up domains by a non-existent system,
					// just return am empty result since there's clearly not
					// going to be any matching domain records.
					return nil, nil
				}
				wheres = append(wheres, fmt.Sprintf("d.system = ANY ($%d)", len(qargs)+1))
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

	var recs []domainRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  d.system AS system_id
, d.id AS domain_id
, d.uuid AS domain_uuid
, d.name AS domain_name
FROM domains AS d
`
		if len(wheres) > 0 {
			qs += "\nWHERE " + strings.Join(wheres, " AND ")
		}
		qs += fmt.Sprintf("\nORDER BY d.uuid ASC LIMIT %d", opts.Limit())
		rows, err := tx.Query(ctx, qs, qargs...)
		if err != nil {
			return errors.Internal(
				"failed reading domain records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[domainRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting domain records",
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
		dom := domain.New(
			domain.WithUUID(rec.UUID),
			domain.WithName(rec.Name),
			domain.WithSystem(sysRec.System),
		)
		out = append(out, &Record{
			RowID:  rec.ID,
			Domain: dom,
		})
	}

	return out, nil
}
