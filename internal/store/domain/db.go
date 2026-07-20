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
	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/system"

	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// dbReadByRowID performs a SELECT query to return the stored domain record
// having the supplied internal DB RowID.
func (s *Store) dbReadByRowID(
	ctx context.Context,
	sysRec storesystem.Record,
	rowID int64,
) (*Record, error) {
	out := Record{
		RowID: rowID,
		Domain: domain.New(
			domain.WithSystem(sysRec.System),
		),
	}
	fn := func(tx pgx.Tx) error {
		var name api.DomainName
		var uuid string
		var rootRowID int64
		var parentRowID sql.NullInt64
		var left int64
		var right int64
		qs := `
SELECT
  uuid
, name
, root
, parent
, left_side
, right_side
FROM domains
WHERE id = $1
`
		err := tx.QueryRow(ctx, qs, rowID).Scan(
			&uuid,
			&name,
			&rootRowID,
			&parentRowID,
			&left,
			&right,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domains record",
				errors.WithWrap(err),
			)
		}
		if parentRowID.Valid {
			// NOTE(jaypipes): This has the potential to do N queries where N
			// is the depth of the domain tree. Consider constraining the
			// behaviour here if we know there is a deep tree.
			parentRec, err := s.ReadByRowID(ctx, sysRec, parentRowID.Int64)
			if err != nil {
				return err
			}
			out.Domain.SetParent(parentRec.Domain)
		}
		if rootRowID != rowID {
			rootDomRec, err := s.ReadByRowID(ctx, sysRec, rootRowID)
			if err != nil {
				return err
			}
			out.Domain.SetRoot(rootDomRec.Domain)
		}
		out.Domain.SetUUID(uuid)
		out.Domain.SetName(name)
		out.Root = rootRowID
		out.Left = left
		out.Right = right
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
	sysRec storesystem.Record,
	uuid string,
) (*Record, error) {
	out := Record{
		Domain: domain.New(
			domain.WithUUID(uuid),
			domain.WithSystem(sysRec.System),
		),
	}
	fn := func(tx pgx.Tx) error {
		var name api.DomainName
		var rootRowID int64
		var parentRowID sql.NullInt64
		var left int64
		var right int64
		qs := `
SELECT
  id
, name
, root
, parent
, left_side
, right_side
FROM domains
WHERE uuid = $1
`
		err := tx.QueryRow(ctx, qs, uuid).Scan(
			&out.RowID,
			&name,
			&rootRowID,
			&parentRowID,
			&left,
			&right,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domains record",
				errors.WithWrap(err),
			)
		}
		if parentRowID.Valid {
			// NOTE(jaypipes): This has the potential to do N queries where N
			// is the depth of the domain tree. Consider constraining the
			// behaviour here if we know there is a deep tree.
			parentRec, err := s.ReadByRowID(ctx, sysRec, parentRowID.Int64)
			if err != nil {
				return err
			}
			out.Domain.SetParent(parentRec.Domain)
		}
		rootDomRec, err := s.ReadByRowID(ctx, sysRec, rootRowID)
		if err != nil {
			return err
		}
		out.Domain.SetRoot(rootDomRec.Domain)
		out.Domain.SetName(name)
		out.Root = rootRowID
		out.Left = left
		out.Right = right
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
	sysRec storesystem.Record,
	name api.DomainName,
) (*Record, error) {
	out := Record{
		Domain: domain.New(
			domain.WithSystem(sysRec.System),
			domain.WithName(name),
		),
	}
	fn := func(tx pgx.Tx) error {
		var uuid string
		var rootRowID int64
		var parentRowID sql.NullInt64
		var left int64
		var right int64
		qs := `
SELECT
  id
, uuid
, root
, parent
, left_side
, right_side
FROM domains
WHERE system = $1
AND name = $2
`
		err := tx.QueryRow(ctx, qs, sysRec.RowID, name).Scan(
			&out.RowID,
			&uuid,
			&rootRowID,
			&parentRowID,
			&left,
			&right,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domains record",
				errors.WithWrap(err),
			)
		}
		if parentRowID.Valid {
			// NOTE(jaypipes): This has the potential to do N queries where N
			// is the depth of the domain tree. Consider constraining the
			// behaviour here if we know there is a deep tree.
			parentRec, err := s.ReadByRowID(ctx, sysRec, parentRowID.Int64)
			if err != nil {
				return err
			}
			out.Domain.SetParent(parentRec.Domain)
		}
		rootDomRec, err := s.ReadByRowID(ctx, sysRec, rootRowID)
		if err != nil {
			return err
		}
		out.Domain.SetRoot(rootDomRec.Domain)
		out.Domain.SetUUID(uuid)
		out.Root = rootRowID
		out.Left = left
		out.Right = right
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
	sysRec storesystem.Record,
	dom api.Domain,
) error {
	parent := dom.Parent()
	if parent == nil {
		return s.dbInsertRoot(ctx, sysRec, dom)
	}
	return s.dbInsertNonRoot(ctx, sysRec, *parent, dom)
}

// dbInsertRoot creates a new domain record for a root node in a "domain tree".
func (s *Store) dbInsertRoot(
	ctx context.Context,
	sysRec storesystem.Record,
	dom api.Domain,
) error {
	left := 1
	right := 2
	createdOn := time.Now().UnixNano()
	caller := api.CallerFromContext(ctx)
	createdBy := caller.Identity
	uuid := dom.UUID()
	name := dom.Name()
	fn := func(tx pgx.Tx) error {
		qs := `
INSERT INTO domains (
  system
, uuid
, name
, root
, left_side
, right_side
, last_modified_on
, last_modified_by
) VALUES (
  $1
, $2
, $3
, lastval()
, $4
, $5
, $6
, $7
)`
		_, err := tx.Exec(
			ctx, qs,
			sysRec.RowID,
			uuid,
			name,
			left,
			right,
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
			"failed inserting root domains record",
			errors.WithWrap(err),
		)
	}
	return nil
}

// dbInsertNonRoot inserts a non-root domain and atomically adjusts the nested
// set model values for the domain tree.
func (s *Store) dbInsertNonRoot(
	ctx context.Context,
	sysRec storesystem.Record,
	parent api.Domain,
	dom api.Domain,
) error {
	parentRec, err := s.ReadByUUID(ctx, sysRec, parent.UUID())
	if err != nil {
		if err == errors.ErrNotFound {
			return errors.ErrDomainParentNotFound
		}
	}

	rootRowID := parentRec.Root
	parentRowID := parentRec.RowID
	parentRight := parentRec.Right
	thisLeft := parentRight
	thisRight := thisLeft + 1

	createdOn := time.Now().UnixNano()
	caller := api.CallerFromContext(ctx)
	createdBy := caller.Identity
	uuid := dom.UUID()
	name := dom.Name()
	fn := func(tx pgx.Tx) error {
		// Before inserting the new node in the domain tree, we need to make
		// spec in the nested sets for our new node.
		qs := `
UPDATE domains SET right_side = right_side + 2
WHERE root = $1 AND right_side >= $2
`
		_, err := tx.Exec(ctx, qs, rootRowID, parentRight)
		if err != nil {
			return fmt.Errorf(
				"failed shifting nested set rights for root domain %d: %w",
				rootRowID, err,
			)
		}
		qs = `
UPDATE domains SET left_side = left_side + 2
WHERE root = $1 AND left_side >= $2
`
		_, err = tx.Exec(ctx, qs, rootRowID, parentRight)
		if err != nil {
			return fmt.Errorf(
				"failed shifting nested set lefts for root domain %d: %w",
				rootRowID, err,
			)
		}
		qs = `
INSERT INTO domains (
  system
, uuid
, name
, root
, parent
, left_side
, right_side
, last_modified_on
, last_modified_by
) VALUES (
  $1
, $2
, $3
, $4
, $5
, $6
, $7
, $8
, $9
)`
		_, err = tx.Exec(
			ctx, qs,
			sysRec.RowID,
			uuid,
			name,
			rootRowID,
			parentRowID,
			thisLeft,
			thisRight,
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
			"failed inserting non-root domains record",
			errors.WithWrap(err),
		)
	}
	return nil
}

type domainRecord struct {
	SystemID  int64          `db:"system_id"`
	ID        int64          `db:"domain_id"`
	UUID      string         `db:"domain_uuid"`
	Name      api.DomainName `db:"domain_name"`
	RootID    int64          `db:"root_id"`
	ParentID  sql.NullInt64  `db:"parent_id"`
	LeftSide  int64          `db:"left_side"`
	RightSide int64          `db:"right_side"`
}

// dbReadByExpression queries zero or more Domains that match the given
// pre-validated expression and options.
func (s *Store) dbReadByExpression(
	ctx context.Context,
	expr query.Expression,
	opts query.Options,
) ([]*Record, error) {
	qargs := []any{}
	wheres := []string{}
	treeOp := false

	switch expr := expr.(type) {
	case query.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case domain.UUIDPredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("d.uuid = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			case query.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("d.uuid = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		case domain.NamePredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("d.name = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			case query.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("d.name = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		case system.UUIDPredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				sysUUID := pred.Value.(string)
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
			case query.PredicateOperatorIn:
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
		case domain.RootUUIDPredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("d.root = (SELECT id FROM domains WHERE uuid = $%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		case domain.RootNamePredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("d.root = (SELECT id FROM domains WHERE name = $%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, errors.UnsupportedPredicateOperator(op)
			}
		case domain.ParentUUIDPredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				treeOp = true
				wheres = append(
					wheres,
					fmt.Sprintf(
						"dset.uuid = $%d AND "+
							"d.left_side BETWEEN dset.left_side AND dset.right_side",
						len(qargs)+1,
					),
				)
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

	var recs []domainRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  d.system AS system_id
, d.id AS domain_id
, d.uuid AS domain_uuid
, d.name AS domain_name
, d.root AS root_id
, d.parent AS parent_id
, d.left_side
, d.right_side
FROM domains AS d`
		if treeOp {
			qs += `
 INNER JOIN domains AS dset
  ON d.root = dset.root`
		}
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
		if rec.ParentID.Valid {
			// NOTE(jaypipes): This has the potential to do N*M queries where N
			// is the limit of records fetched and M is the the depth of the
			// domain tree of that domain record. Consider constraining the
			// behaviour here if we know there is a deep tree.
			parentRec, err := s.ReadByRowID(ctx, *sysRec, rec.ParentID.Int64)
			if err != nil {
				return nil, err
			}
			dom.SetParent(parentRec.Domain)
		}
		rootDomRec, err := s.ReadByRowID(ctx, *sysRec, rec.RootID)
		if err != nil {
			return nil, err
		}
		dom.SetRoot(rootDomRec.Domain)
		out = append(out, &Record{
			RowID:  rec.ID,
			Root:   rec.RootID,
			Left:   rec.LeftSide,
			Right:  rec.RightSide,
			Domain: dom,
		})
	}

	return out, nil
}

// dbReadDomainsInTreeByRootRowID returns the set of Records comprising the
// "domain tree" rooted at the supplied root domain row ID.
func (s *Store) dbReadDomainsInTreeByRootRowID(
	ctx context.Context,
	rootRowID int64,
) ([]*Record, error) {
	var recs []domainRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  d.system AS system_id
, d.id AS domain_id
, d.uuid AS domain_uuid
, d.name AS domain_name
, d.root AS root_id
, d.parent AS parent_id
, d.left_side
, d.right_side
FROM domains AS d
WHERE d.root = $1
`
		rows, err := tx.Query(ctx, qs, rootRowID)
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
		if rec.ParentID.Valid {
			// NOTE(jaypipes): This has the potential to do N queries where N
			// is the depth of the domain tree. Consider constraining the
			// behaviour here if we know there is a deep tree.
			parentRec, err := s.ReadByRowID(ctx, *sysRec, rec.ParentID.Int64)
			if err != nil {
				return nil, err
			}
			dom.SetParent(parentRec.Domain)
		}
		rootDomRec, err := s.ReadByRowID(ctx, *sysRec, rec.RootID)
		if err != nil {
			return nil, err
		}
		dom.SetRoot(rootDomRec.Domain)
		out = append(out, &Record{
			RowID:  rec.ID,
			Root:   rootRowID,
			Left:   rec.LeftSide,
			Right:  rec.RightSide,
			Domain: dom,
		})
	}

	return out, nil
}
