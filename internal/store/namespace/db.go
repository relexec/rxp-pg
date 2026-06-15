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
	"github.com/relexec/rxp/namespace"
	"github.com/relexec/rxp/query"

	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
)

// dbReadByRowID performs a SELECT query to return the stored namespace record
// having the supplied internal DB RowID.
func (s *Store) dbReadByRowID(
	ctx context.Context,
	rowID int64,
) (*Record, error) {
	out := Record{
		RowID: rowID,
	}
	fn := func(tx pgx.Tx) error {
		var domainRowID int64
		var name api.NamespaceName
		var uuid string
		qs := "SELECT domain, uuid, name FROM namespaces WHERE id = $1"
		err := tx.QueryRow(ctx, qs, rowID).Scan(&domainRowID, &uuid, &name)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading namespaces record",
				errors.WithWrap(err),
			)
		}
		domainRec, err := s.domainStore.ReadByRowID(ctx, domainRowID)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domain record for namespace",
				errors.WithWrap(err),
			)
		}
		out.Namespace = namespace.New(
			namespace.WithDomain(domainRec.Domain),
			namespace.WithName(name),
			namespace.WithUUID(uuid),
		)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByUUID performs a SELECT query to return the stored namespace record
// having the supplied UUID.
func (s *Store) dbReadByUUID(
	ctx context.Context,
	uuid string,
) (*Record, error) {
	out := Record{
		Namespace: namespace.New(
			namespace.WithUUID(uuid),
		),
	}
	fn := func(tx pgx.Tx) error {
		var domainRowID int64
		var name api.NamespaceName
		qs := "SELECT domain, id, name FROM namespaces WHERE uuid = $1"
		err := tx.QueryRow(ctx, qs, uuid).Scan(&domainRowID, &out.RowID, &name)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading namespaces record",
				errors.WithWrap(err),
			)
		}
		domainRec, err := s.domainStore.ReadByRowID(ctx, domainRowID)
		if err != nil {
			return errors.Internal(
				"failed reading domain record for namespace",
				errors.WithWrap(err),
			)
		}
		out.Namespace.SetName(name)
		out.Namespace.SetDomain(domainRec.Domain)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByName performs a SELECT query to return the stored namespace record
// having the supplied Name.
func (s *Store) dbReadByName(
	ctx context.Context,
	domainRec *storedomain.Record,
	name api.NamespaceName,
) (*Record, error) {
	out := Record{
		Namespace: namespace.New(
			namespace.WithDomain(domainRec.Domain),
			namespace.WithName(name),
		),
	}
	fn := func(tx pgx.Tx) error {
		var uuid string
		qs := "SELECT id, uuid FROM namespaces WHERE domain = $1 AND name = $2"
		err := tx.QueryRow(ctx, qs, domainRec.RowID, name).Scan(&out.RowID, &uuid)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading namespaces record",
				errors.WithWrap(err),
			)
		}
		out.Namespace.SetUUID(uuid)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbInsert atomically writes the supplied Namespace to persistent storage.
func (s *Store) dbInsert(
	ctx context.Context,
	domainRec *storedomain.Record,
	dom *namespace.Namespace,
) error {
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	uuid := dom.UUID()
	name := dom.Name()
	fn := func(tx pgx.Tx) error {
		qs := `
INSERT INTO namespaces (
  domain
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
			domainRec.RowID,
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
						return errors.DuplicateKey("namespace", "uuid", uuid)
					} else {
						return errors.DuplicateName("namespace", name)
					}
				}
			}
		}
		return err
	}
	if err := s.Exec(ctx, fn); err != nil {
		return errors.Internal(
			"failed inserting namespaces record",
			errors.WithWrap(err),
		)
	}
	return nil
}

type namespaceRecord struct {
	DomainID int64             `db:"domain_id"`
	ID       int64             `db:"namespace_id"`
	UUID     string            `db:"namespace_uuid"`
	Name     api.NamespaceName `db:"namespace_name"`
}

// dbReadByExpression queries zero or more Namespaces that match the given
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
		case namespace.UUIDPredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("n.uuid = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			case query.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("n.uuid = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		case namespace.NamePredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("n.name = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			case query.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("n.name = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		case domain.UUIDPredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				domUUID := pred.Value.(string)
				domRec, err := s.domainStore.ReadByUUID(ctx, domUUID)
				if err != nil {
					// If we're looking up namespaces by a non-existent domain,
					// just return am empty result since there's clearly not
					// going to be any matching namespace records.
					if err == errors.ErrNotFound {
						return nil, nil
					}
					return nil, err
				}
				wheres = append(wheres, fmt.Sprintf("n.domain = $%d", len(qargs)+1))
				qargs = append(qargs, domRec.RowID)
			case query.PredicateOperatorIn:
				domRowIDs := []int64{}
				domUUIDs := pred.Value.([]string)
				for _, domUUID := range domUUIDs {
					domRec, err := s.domainStore.ReadByUUID(ctx, domUUID)
					if err != nil {
						if err == errors.ErrNotFound {
							continue
						}
						return nil, err
					}
					domRowIDs = append(domRowIDs, domRec.RowID)
				}
				if len(domRowIDs) == 0 {
					// If we're looking up namespaces by a non-existent domain,
					// just return am empty result since there's clearly not
					// going to be any matching namespace records.
					return nil, nil
				}
				wheres = append(wheres, fmt.Sprintf("n.domain = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, domRowIDs)
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		case domain.NamePredicate:
			// NOTE(jaypipes): DomainNamePredicate is used when the caller only
			// knows the domain name and not the domain's UUID and domain's
			// System. Therefore, we assume the host system for these cases. In
			// cases when the caller knows the domain name AND System but not
			// the domain's UUID, the DomainPredicate is used and we don't need
			// to assume the host system.
			sys := s.hostSystemRecord.System
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				domName := pred.Value.(api.DomainName)
				domRec, err := s.domainStore.ReadByName(ctx, sys, domName)
				if err != nil {
					// If we're looking up namespaces by a non-existent domain,
					// just return am empty result since there's clearly not
					// going to be any matching namespace records.
					if err == errors.ErrNotFound {
						return nil, nil
					}
					return nil, err
				}
				wheres = append(wheres, fmt.Sprintf("n.domain = $%d", len(qargs)+1))
				qargs = append(qargs, domRec.RowID)
			case query.PredicateOperatorIn:
				domRowIDs := []int64{}
				domNames := pred.Value.([]api.DomainName)
				for _, domName := range domNames {
					domRec, err := s.domainStore.ReadByName(ctx, sys, domName)
					if err != nil {
						if err == errors.ErrNotFound {
							continue
						}
						return nil, err
					}
					domRowIDs = append(domRowIDs, domRec.RowID)
				}
				if len(domRowIDs) == 0 {
					// If we're looking up namespaces by a non-existent domain,
					// just return am empty result since there's clearly not
					// going to be any matching namespace records.
					return nil, nil
				}
				wheres = append(wheres, fmt.Sprintf("n.domain = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, domRowIDs)
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		case domain.DomainPredicate:
			// NOTE(jaypipes): DomainPredicate is used when the caller only
			// knows the domain name AND system but does NOT know the domain's
			// UUID.
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				dom := pred.Value.(*domain.Domain)
				sys := dom.System()
				if sys == nil {
					sys = s.hostSystemRecord.System
				}
				domRec, err := s.domainStore.ReadByName(ctx, sys, dom.Name())
				if err != nil {
					// If we're looking up namespaces by a non-existent domain,
					// just return am empty result since there's clearly not
					// going to be any matching namespace records.
					if err == errors.ErrNotFound {
						return nil, nil
					}
					return nil, err
				}
				wheres = append(wheres, fmt.Sprintf("n.domain = $%d", len(qargs)+1))
				qargs = append(qargs, domRec.RowID)
			case query.PredicateOperatorIn:
				domRowIDs := []int64{}
				doms := pred.Value.([]*domain.Domain)
				for _, dom := range doms {
					sys := dom.System()
					if sys == nil {
						sys = s.hostSystemRecord.System
					}
					domRec, err := s.domainStore.ReadByName(ctx, sys, dom.Name())
					if err != nil {
						if err == errors.ErrNotFound {
							continue
						}
						return nil, err
					}
					domRowIDs = append(domRowIDs, domRec.RowID)
				}
				if len(domRowIDs) == 0 {
					// If we're looking up namespaces by a non-existent domain,
					// just return am empty result since there's clearly not
					// going to be any matching namespace records.
					return nil, nil
				}
				wheres = append(wheres, fmt.Sprintf("n.domain = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, domRowIDs)
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		default:
			return nil, errors.UnsupportedPredicate(pred)
		}
	default:
		return nil, errors.UnsupportedExpression(expr)
	}

	var recs []namespaceRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  n.domain AS domain_id
, n.id AS namespace_id
, n.uuid AS namespace_uuid
, n.name AS namespace_name
FROM namespaces AS n
`
		if len(wheres) > 0 {
			qs += "\nWHERE " + strings.Join(wheres, " AND ")
		}
		qs += fmt.Sprintf("\nORDER BY n.uuid ASC LIMIT %d", opts.Limit())
		rows, err := tx.Query(ctx, qs, qargs...)
		if err != nil {
			return errors.Internal(
				"failed reading namespace records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[namespaceRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting namespace records",
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
		domRec, err := s.domainStore.ReadByRowID(ctx, rec.DomainID)
		if err != nil {
			return nil, errors.Internal(
				"failed reading domain record by rowid",
				errors.WithWrap(err),
			)
		}
		ns := namespace.New(
			namespace.WithUUID(rec.UUID),
			namespace.WithName(rec.Name),
			namespace.WithDomain(domRec.Domain),
		)
		out = append(out, &Record{
			RowID:     rec.ID,
			Namespace: ns,
		})
	}

	return out, nil
}
