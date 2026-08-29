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
	apicore "github.com/relexec/rxp/api/core"
	rxpdomain "github.com/relexec/rxp/api/domain"
	apierrors "github.com/relexec/rxp/api/errors"
	rxpquery "github.com/relexec/rxp/api/query"
	rxpsystem "github.com/relexec/rxp/api/system"
)

// dbReadByRowID performs a SELECT query to return the stored domain record
// having the supplied internal DB RowID.
func (s *Store) dbReadByRowID(
	ctx context.Context,
	sysRec *rxpsystem.System,
	rowID int64,
) (*rxpdomain.Domain, error) {
	out := &rxpdomain.Domain{
		System: sysRec,
	}
	out.SetSystemInternalID(rowID)
	fn := func(tx pgx.Tx) error {
		var name rxpdomain.Name
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
				return apierrors.ErrNotFound
			}
			return apierrors.Internal(
				"failed reading domains record by rowid",
				apierrors.WithWrap(err),
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
			out.Parent = parentRec
		}
		if rootRowID != rowID {
			rootDomRec, err := s.ReadByRowID(ctx, sysRec, rootRowID)
			if err != nil {
				return err
			}
			out.Root = rootDomRec
		}
		out.UUID = uuid
		out.Name = name
		out.SetNestedSet(left, right)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return out, nil
}

// dbReadByUUID performs a SELECT query to return the stored domain record
// having the supplied UUID.
func (s *Store) dbReadByUUID(
	ctx context.Context,
	sysRec *rxpsystem.System,
	uuid string,
) (*rxpdomain.Domain, error) {
	out := &rxpdomain.Domain{
		UUID:   uuid,
		System: sysRec,
	}
	fn := func(tx pgx.Tx) error {
		var rowID int64
		var name rxpdomain.Name
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
			&rowID,
			&name,
			&rootRowID,
			&parentRowID,
			&left,
			&right,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return apierrors.ErrNotFound
			}
			return apierrors.Internal(
				"failed reading domains record by uuid",
				apierrors.WithWrap(err),
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
			out.Parent = parentRec
		}
		if rootRowID != rowID {
			rootDomRec, err := s.ReadByRowID(ctx, sysRec, rootRowID)
			if err != nil {
				return err
			}
			out.Root = rootDomRec
		}
		out.Name = name
		out.SetSystemInternalID(rowID)
		out.SetNestedSet(left, right)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return out, nil
}

// dbReadByName performs a SELECT query to return the stored domain record
// having the supplied Name.
func (s *Store) dbReadByName(
	ctx context.Context,
	sysRec *rxpsystem.System,
	name rxpdomain.Name,
) (*rxpdomain.Domain, error) {
	sysRowID := sysRec.SystemInternalIDInt64()
	out := &rxpdomain.Domain{
		System: sysRec,
		Name:   name,
	}
	fn := func(tx pgx.Tx) error {
		var rowID int64
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
		err := tx.QueryRow(ctx, qs, sysRowID, name).Scan(
			&rowID,
			&uuid,
			&rootRowID,
			&parentRowID,
			&left,
			&right,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return apierrors.ErrNotFound
			}
			return apierrors.Internal(
				"failed reading domains record by name",
				apierrors.WithWrap(err),
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
			out.Parent = parentRec
		}
		if rootRowID != rowID {
			rootDomRec, err := s.ReadByRowID(ctx, sysRec, rootRowID)
			if err != nil {
				return err
			}
			out.Root = rootDomRec
		}
		out.UUID = uuid
		out.Name = name
		out.SetSystemInternalID(rowID)
		out.SetNestedSet(left, right)
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return out, nil
}

// dbInsert atomically writes the supplied Domain to persistent storage.
func (s *Store) dbInsert(
	ctx context.Context,
	sysRec *rxpsystem.System,
	dom rxpdomain.Domain,
) error {
	parent := dom.Parent
	if parent == nil {
		return s.dbInsertRoot(ctx, sysRec, dom)
	}
	return s.dbInsertNonRoot(ctx, sysRec, *parent, dom)
}

// dbInsertRoot creates a new domain record for a root node in a "domain tree".
func (s *Store) dbInsertRoot(
	ctx context.Context,
	sysRec *rxpsystem.System,
	dom rxpdomain.Domain,
) error {
	sysRowID := sysRec.SystemInternalIDInt64()
	left := 1
	right := 2
	createdOn := time.Now().UnixNano()
	caller := apicore.CallerFromContext(ctx)
	createdBy := caller.Identity
	uuid := dom.UUID
	name := dom.Name
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
			sysRowID,
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
						return apierrors.DuplicateKey("domain", "uuid", uuid)
					} else {
						return apierrors.DuplicateName("domain", name)
					}
				}
			}
		}
		return err
	}
	if err := s.Exec(ctx, fn); err != nil {
		return apierrors.Internal(
			"failed inserting root domains record",
			apierrors.WithWrap(err),
		)
	}
	return nil
}

// dbInsertNonRoot inserts a non-root domain and atomically adjusts the nested
// set model values for the domain tree.
func (s *Store) dbInsertNonRoot(
	ctx context.Context,
	sysRec *rxpsystem.System,
	parent rxpdomain.Domain,
	dom rxpdomain.Domain,
) error {
	sysRowID := sysRec.SystemInternalIDInt64()
	if !parent.HasSystemInternalID() {
		return fmt.Errorf(
			"parent does not have system internal id set",
		)
	}
	parentRowID := parent.SystemInternalIDInt64()
	var rootRowID int64
	parentRoot := parent.Root
	if parentRoot == nil {
		// the parent IS the root and we already verified the parent has a
		// system internal ID.
		rootRowID = parent.SystemInternalIDInt64()
	} else {
		// the parent is NOT the root, so grab the parent's root system
		// internal ID
		if !parentRoot.HasSystemInternalID() {
			return fmt.Errorf(
				"parent's root does not have system internal id set",
			)
		}
		rootRowID = parentRoot.SystemInternalIDInt64()
	}
	parentRight := parent.NestedSetRight()
	thisLeft := parentRight
	thisRight := thisLeft + 1

	createdOn := time.Now().UnixNano()
	caller := apicore.CallerFromContext(ctx)
	createdBy := caller.Identity
	uuid := dom.UUID
	name := dom.Name
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
			sysRowID,
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
						return apierrors.DuplicateKey("domain", "uuid", uuid)
					} else {
						return apierrors.DuplicateName("domain", name)
					}
				}
			}
		}
		return err
	}
	if err := s.Exec(ctx, fn); err != nil {
		return apierrors.Internal(
			"failed inserting non-root domains record",
			apierrors.WithWrap(err),
		)
	}
	return nil
}

type domainRecord struct {
	SystemID  int64          `db:"system_id"`
	ID        int64          `db:"domain_id"`
	UUID      string         `db:"domain_uuid"`
	Name      rxpdomain.Name `db:"domain_name"`
	RootID    int64          `db:"root_id"`
	ParentID  sql.NullInt64  `db:"parent_id"`
	LeftSide  int64          `db:"left_side"`
	RightSide int64          `db:"right_side"`
}

// dbReadByExpression queries zero or more Domains that match the given
// pre-validated expression and options.
func (s *Store) dbReadByExpression(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) ([]*rxpdomain.Domain, error) {
	qargs := []any{}
	wheres := []string{}
	treeOp := false

	switch expr := expr.(type) {
	case rxpquery.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case rxpdomain.UUIDPredicate:
			op := pred.Op
			switch op {
			case rxpquery.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("d.uuid = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			case rxpquery.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("d.uuid = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, apierrors.UnsupportedPredicateOperator(op)
			}
		case rxpdomain.NamePredicate:
			op := pred.Op
			switch op {
			case rxpquery.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("d.name = $%d", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			case rxpquery.PredicateOperatorIn:
				wheres = append(wheres, fmt.Sprintf("d.name = ANY ($%d)", len(qargs)+1))
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
					// If we're looking up domains by a non-existent system,
					// just return am empty result since there's clearly not
					// going to be any matching domain records.
					if err == apierrors.ErrNotFound {
						return nil, nil
					}
					return nil, err
				}
				sysRowID := sysRec.SystemInternalIDInt64()
				wheres = append(wheres, fmt.Sprintf("d.system = $%d", len(qargs)+1))
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
					// If we're looking up domains by a non-existent system,
					// just return am empty result since there's clearly not
					// going to be any matching domain records.
					return nil, nil
				}
				wheres = append(wheres, fmt.Sprintf("d.system = ANY ($%d)", len(qargs)+1))
				qargs = append(qargs, sysRowIDs)
			default:
				return nil, apierrors.UnsupportedPredicateOperator(op)
			}
		case rxpdomain.RootUUIDPredicate:
			op := pred.Op
			switch op {
			case rxpquery.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("d.root = (SELECT id FROM domains WHERE uuid = $%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, apierrors.UnsupportedPredicateOperator(op)
			}
		case rxpdomain.RootNamePredicate:
			op := pred.Op
			switch op {
			case rxpquery.PredicateOperatorEqual:
				wheres = append(wheres, fmt.Sprintf("d.root = (SELECT id FROM domains WHERE name = $%d)", len(qargs)+1))
				qargs = append(qargs, pred.Value)
			default:
				return nil, apierrors.UnsupportedPredicateOperator(op)
			}
		case rxpdomain.ParentUUIDPredicate:
			op := pred.Op
			switch op {
			case rxpquery.PredicateOperatorEqual:
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
				return nil, apierrors.UnsupportedPredicateOperator(op)
			}
		default:
			return nil, apierrors.UnsupportedPredicate(pred)
		}
	default:
		return nil, apierrors.UnsupportedExpression(expr)
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
			return apierrors.Internal(
				"failed reading domain records",
				apierrors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[domainRecord])
		if err != nil {
			return apierrors.Internal(
				"failed collecting domain records",
				apierrors.WithWrap(err),
			)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}

	out := make([]*rxpdomain.Domain, 0, len(recs))
	for _, rec := range recs {
		sysRec, err := s.systemStore.ReadByRowID(ctx, rec.SystemID)
		if err != nil {
			return nil, apierrors.Internal(
				"failed reading system record by rowid",
				apierrors.WithWrap(err),
			)
		}
		dom := &rxpdomain.Domain{
			UUID:   rec.UUID,
			Name:   rec.Name,
			System: sysRec,
		}
		dom.SetSystemInternalID(rec.ID)
		if rec.ParentID.Valid {
			// NOTE(jaypipes): This has the potential to do N*M queries where N
			// is the limit of records fetched and M is the the depth of the
			// domain tree of that domain record. Consider constraining the
			// behaviour here if we know there is a deep tree.
			parentRec, err := s.ReadByRowID(ctx, sysRec, rec.ParentID.Int64)
			if err != nil {
				return nil, err
			}
			dom.Parent = parentRec
		}
		if rec.RootID != rec.ID {
			rootDomRec, err := s.ReadByRowID(ctx, sysRec, rec.RootID)
			if err != nil {
				return nil, err
			}
			dom.Root = rootDomRec
		}
		dom.SetNestedSet(rec.LeftSide, rec.RightSide)
		out = append(out, dom)
	}

	return out, nil
}

// dbReadDomainsInTreeByRootRowID returns the set of rxpdomain.Domains comprising the
// "domain tree" rooted at the supplied root domain row ID.
func (s *Store) dbReadDomainsInTreeByRootRowID(
	ctx context.Context,
	rootRowID int64,
) ([]*rxpdomain.Domain, error) {
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
			return apierrors.Internal(
				"failed reading domain records in tree by root rowid",
				apierrors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[domainRecord])
		if err != nil {
			return apierrors.Internal(
				"failed collecting domain records in tree by root rowid",
				apierrors.WithWrap(err),
			)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}

	out := make([]*rxpdomain.Domain, 0, len(recs))
	for _, rec := range recs {
		sysRec, err := s.systemStore.ReadByRowID(ctx, rec.SystemID)
		if err != nil {
			return nil, apierrors.Internal(
				"failed reading system record by rowid",
				apierrors.WithWrap(err),
			)
		}
		dom := &rxpdomain.Domain{
			UUID:   rec.UUID,
			Name:   rec.Name,
			System: sysRec,
		}
		dom.SetSystemInternalID(rec.ID)
		if rec.ParentID.Valid {
			// NOTE(jaypipes): This has the potential to do N queries where N
			// is the depth of the domain tree. Consider constraining the
			// behaviour here if we know there is a deep tree.
			parentRec, err := s.ReadByRowID(ctx, sysRec, rec.ParentID.Int64)
			if err != nil {
				return nil, err
			}
			dom.Parent = parentRec
		}
		if rec.RootID != rec.ID {
			rootDomRec, err := s.ReadByRowID(ctx, sysRec, rec.RootID)
			if err != nil {
				return nil, err
			}
			dom.Root = rootDomRec
		}
		dom.SetNestedSet(rec.LeftSide, rec.RightSide)
		out = append(out, dom)
	}

	return out, nil
}
