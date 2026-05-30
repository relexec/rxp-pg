package store

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/jackc/pgx/v5"
	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"
	"github.com/relexec/rxp/system"
)

// Query queries zero or more Objects from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr expression.Expression,
	opts query.Options,
) ([]*Record, error) {
	var err error
	var sqRecords []systemQualifiedObjectRecord

	sys := s.hostSystemRecord.System

	var nsqExpr expression.Expression
	var dqExpr expression.Expression
	var sqExpr expression.Expression

	switch expr := expr.(type) {
	case expression.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case expression.KindNamePredicate:
			op := pred.Operator()
			switch op {
			case expression.PredicateOperatorEqual:
				kn := pred.Value().(api.KindName)
				kindRec, err := s.kindStore.ReadByName(ctx, sys, kn)
				if err != nil {
					return nil, err
				}
				scope := kindRec.Kind.Scope()
				switch scope {
				case api.ScopeNamespace:
					nsqExpr = expr
				case api.ScopeDomain:
					dqExpr = expr
				default:
					sqExpr = expr
				}
			}
		}
	case expression.OrExpression:
	case expression.AndExpression:
	}

	out := []*Record{}
	if nsqExpr != nil {
		nsqRecords, err := s.dbReadNamespaceQualifiedByExpression(
			ctx, nsqExpr, opts,
		)
		if err != nil {
			return nil, err
		}
		for _, rec := range nsqRecords {
			out = append(out, rec)
		}
	}
	if dqExpr != nil {
		dqRecords, err := s.dbReadDomainQualifiedByExpression(
			ctx, dqExpr, opts,
		)
		if err != nil {
			return nil, err
		}
		for _, rec := range dqRecords {
			out = append(out, rec)
		}
	}
	if sqExpr != nil {
		sqRecords, err = s.objectQuerySystemQualified(
			ctx, sqExpr, opts,
		)
		if err != nil {
			return nil, err
		}
		for _, rec := range sqRecords {
			sv, err := semver.NewVersion(rec.KindVersionVersion)
			if err != nil {
				return nil, err
			}
			kv := api.NewKindVersionName(rec.KindName, *sv)
			sys := system.New(system.WithUUID(rec.SystemUUID))
			obj := object.New(
				object.WithSystem(sys),
				object.WithKindVersionName(kv),
			)
			entry := &Record{
				RowID:  rec.ID,
				Object: obj,
			}
			out = append(out, entry)
		}
	}

	if len(out) > opts.Limit() {
		// We may have more than the limit because we read
		// differently-qualified object records with the same limit.
		slices.SortFunc(out, func(a, b *Record) int {
			return strings.Compare(a.Object.UUID(), b.Object.UUID())
		})
		out = out[0:opts.Limit()]
	}
	return out, nil
}

type systemQualifiedObjectRecord struct {
	ID                 int64        `db:"object_id"`
	UUID               string       `db:"object_uuid"`
	Generation         int64        `db:"object_generation"`
	ObjectName         string       `db:"object_name"`
	SystemID           int64        `db:"system_id"`
	SystemUUID         string       `db:"system_uuid"`
	KindID             int64        `db:"kind_id"`
	KindName           api.KindName `db:"kind_name"`
	KindVersionID      int64        `db:"kindversion_id"`
	KindVersionVersion string       `db:"kindversion_version"`
}

// objectQuerySystemQualified queries zero or more Objects that have
// system-qualified names from persistent storage given the pre-validated
// expression and options.
func (s *Store) objectQuerySystemQualified(
	ctx context.Context,
	expr expression.Expression,
	opts query.Options,
) ([]systemQualifiedObjectRecord, error) {
	sysRec := s.hostSystemRecord
	sys := sysRec.System

	qargs := []any{
		sysRec.RowID,
	}
	wheres := []string{
		"o.system = $1",
	}

	switch expr := expr.(type) {
	case expression.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case expression.KindNamePredicate:
			op := pred.Operator()
			switch op {
			case expression.PredicateOperatorEqual:
				kn := pred.Value().(api.KindName)
				kindRec, err := s.kindStore.ReadByName(ctx, sys, kn)
				if err != nil {
					return nil, err
				}
				wheres = append(wheres, fmt.Sprintf("kv.kind = $%d", len(qargs)+1))
				qargs = append(qargs, kindRec.RowID)
			}
		}
	case expression.OrExpression:
	case expression.AndExpression:
	}
	var recs []systemQualifiedObjectRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.uuid AS object_uuid
, o.generation AS object_generation
, n.name AS object_name
, o.system AS system_id
, s.uuid AS system_uuid
, o.kindversion AS kindversion_id
, kv.version AS kindversion_version
, kv.kind AS kind_id
, k.name AS kind_name
FROM objects AS o
 INNER JOIN systems AS s
  ON o.system = s.id
 INNER JOIN kindversions AS kv
  ON o.kindversion = kv.id
 INNER JOIN kinds AS k
  ON kv.kind = k.id
 INNER JOIN system_qualified_object_names AS n
  ON o.id = n.object
`
		if len(wheres) > 0 {
			qs += "\nWHERE " + strings.Join(wheres, " AND ")
		}
		qs += fmt.Sprintf("\nORDER BY o.uuid ASC LIMIT %d", opts.Limit())
		rows, err := tx.Query(ctx, qs, qargs...)
		if err != nil {
			return errors.Internal(
				"failed reading system-qualified object records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[systemQualifiedObjectRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting system-qualified object records",
				errors.WithWrap(err),
			)
		}

		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return recs, nil
}
