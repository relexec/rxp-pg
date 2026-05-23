package store

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/jackc/pgx/v5"
	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/list/expression"
	"github.com/relexec/rxp/list/option"
	"github.com/relexec/rxp/namespace"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/system"
	"github.com/relexec/rxp/types"
)

// Query lists zero or more Objects from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr types.Expression,
	opts option.Options,
) ([]*Record, error) {
	var err error
	var nsqRecords []namespaceQualifiedObjectRecord
	var dqRecords []domainQualifiedObjectRecord
	var sqRecords []systemQualifiedObjectRecord

	sys := s.hostSystemRecord.System

	var nsqExpr types.Expression
	var dqExpr types.Expression
	var sqExpr types.Expression

	switch expr := expr.(type) {
	case expression.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case expression.KindNamePredicate:
			v := pred.Values()[0]
			kn := v.(api.KindName)
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
	case expression.OrExpression:
	case expression.AndExpression:
	}

	out := []*Record{}
	if nsqExpr != nil {
		nsqRecords, err = s.objectListNamespaceQualified(
			ctx, nsqExpr, opts,
		)
		if err != nil {
			return nil, err
		}
		for _, rec := range nsqRecords {
			sv, err := semver.NewVersion(rec.MetaVersion)
			if err != nil {
				return nil, err
			}
			kv := api.NewKindVersion(rec.KindName, *sv)
			sys := system.New(system.WithUUID(rec.SystemUUID))
			obj := object.New(
				object.WithSystem(sys),
				object.WithKindVersion(kv),
			)
			dom := domain.New(
				domain.WithSystem(sys),
				domain.WithName(rec.DomainName),
			)
			obj.SetDomain(dom)
			ns := namespace.New(
				namespace.WithDomain(obj.Domain()),
				namespace.WithName(rec.NamespaceName),
			)
			obj.SetNamespace(ns)
			entry := &Record{
				RowID:  rec.ID,
				Object: obj,
			}
			out = append(out, entry)
		}
	}
	if dqExpr != nil {
		dqRecords, err = s.objectListDomainQualified(
			ctx, dqExpr, opts,
		)
		if err != nil {
			return nil, err
		}
		for _, rec := range dqRecords {
			sv, err := semver.NewVersion(rec.MetaVersion)
			if err != nil {
				return nil, err
			}
			kv := api.NewKindVersion(rec.KindName, *sv)
			sys := system.New(system.WithUUID(rec.SystemUUID))
			obj := object.New(
				object.WithSystem(sys),
				object.WithKindVersion(kv),
			)
			dom := domain.New(
				domain.WithSystem(sys),
				domain.WithName(rec.DomainName),
			)
			obj.SetDomain(dom)
			entry := &Record{
				RowID:  rec.ID,
				Object: obj,
			}
			out = append(out, entry)
		}
	}
	if sqExpr != nil {
		sqRecords, err = s.objectListSystemQualified(
			ctx, sqExpr, opts,
		)
		if err != nil {
			return nil, err
		}
		for _, rec := range sqRecords {
			sv, err := semver.NewVersion(rec.MetaVersion)
			if err != nil {
				return nil, err
			}
			kv := api.NewKindVersion(rec.KindName, *sv)
			sys := system.New(system.WithUUID(rec.SystemUUID))
			obj := object.New(
				object.WithSystem(sys),
				object.WithKindVersion(kv),
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

type namespaceQualifiedObjectRecord struct {
	ID            int64             `db:"object_id"`
	UUID          string            `db:"object_uuid"`
	Generation    int64             `db:"object_generation"`
	ObjectName    string            `db:"object_name"`
	SystemID      int64             `db:"system_id"`
	SystemUUID    string            `db:"system_uuid"`
	KindID        int64             `db:"kind_id"`
	KindName      api.KindName      `db:"kind_name"`
	MetaID        int64             `db:"meta_id"`
	MetaVersion   string            `db:"meta_version"`
	DomainID      int64             `db:"domain_id"`
	DomainName    api.DomainName    `db:"domain_name"`
	NamespaceID   int64             `db:"namespace_id"`
	NamespaceName api.NamespaceName `db:"namespace_name"`
}

// objectListNamespaceQualified lists zero or more Objects that have
// namespace-qualified names from persistent storage given the pre-validated
// expression and options.
func (s *Store) objectListNamespaceQualified(
	ctx context.Context,
	expr types.Expression,
	opts option.Options,
) ([]namespaceQualifiedObjectRecord, error) {
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
			v := pred.Values()[0]
			kn := v.(api.KindName)
			kindRec, err := s.kindStore.ReadByName(ctx, sys, kn)
			if err != nil {
				return nil, err
			}
			wheres = append(wheres, fmt.Sprintf("m.kind = $%d", len(qargs)+1))
			qargs = append(qargs, kindRec.RowID)
		}
	case expression.OrExpression:
	case expression.AndExpression:
	}

	var recs []namespaceQualifiedObjectRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.uuid AS object_uuid
, o.generation AS object_generation
, n.name AS object_name
, o.system AS system_id
, s.uuid AS system_uuid
, o.meta AS meta_id
, m.version AS meta_version
, m.kind AS kind_id
, k.name AS kind_name
, o.domain AS domain_id
, d.name AS domain_name
, o.namespace AS namespace_id
, ns.name AS namespace_name
FROM objects AS o
 INNER JOIN systems AS s
  ON o.system = s.id
 INNER JOIN metas AS m
  ON o.meta = m.id
 INNER JOIN kinds AS k
  ON m.kind = k.id
 INNER JOIN domains AS d
  ON o.domain = d.id
 INNER JOIN namespaces AS ns
  ON o.namespace = ns.id
 INNER JOIN namespace_qualified_object_names AS n
  ON o.id = n.object
`
		if len(wheres) > 0 {
			qs += "\nWHERE " + strings.Join(wheres, " AND ")
		}
		qs += fmt.Sprintf("\nORDER BY o.uuid ASC LIMIT %d", opts.Limit())
		rows, err := tx.Query(ctx, qs, qargs...)
		if err != nil {
			return errors.Internal(
				"failed reading namespace-qualified object records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[namespaceQualifiedObjectRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting namespace-qualified object records",
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

type domainQualifiedObjectRecord struct {
	ID          int64          `db:"object_id"`
	UUID        string         `db:"object_uuid"`
	Generation  int64          `db:"object_generation"`
	ObjectName  string         `db:"object_name"`
	SystemID    int64          `db:"system_id"`
	SystemUUID  string         `db:"system_uuid"`
	KindID      int64          `db:"kind_id"`
	KindName    api.KindName   `db:"kind_name"`
	MetaID      int64          `db:"meta_id"`
	MetaVersion string         `db:"meta_version"`
	DomainID    int64          `db:"domain_id"`
	DomainName  api.DomainName `db:"domain_name"`
}

// objectListDomainQualified lists zero or more Objects that have
// domain-qualified names from persistent storage given the pre-validated
// expression and options.
func (s *Store) objectListDomainQualified(
	ctx context.Context,
	expr types.Expression,
	opts option.Options,
) ([]domainQualifiedObjectRecord, error) {
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
			v := pred.Values()[0]
			kn := v.(api.KindName)
			kindRec, err := s.kindStore.ReadByName(ctx, sys, kn)
			if err != nil {
				return nil, err
			}
			wheres = append(wheres, fmt.Sprintf("m.kind = $%d", len(qargs)+1))
			qargs = append(qargs, kindRec.RowID)
		}
	case expression.OrExpression:
	case expression.AndExpression:
	}
	var recs []domainQualifiedObjectRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  o.id AS object_id
, o.uuid AS object_uuid
, o.generation AS object_generation
, n.name AS object_name
, o.system AS system_id
, s.uuid AS system_uuid
, o.meta AS meta_id
, m.version AS meta_version
, m.kind AS kind_id
, k.name AS kind_name
, o.domain AS domain_id
, d.name AS domain_name
FROM objects AS o
 INNER JOIN systems AS s
  ON o.system = s.id
 INNER JOIN metas AS m
  ON o.meta = m.id
 INNER JOIN kinds AS k
  ON m.kind = k.id
 INNER JOIN domains AS d
  ON o.domain = d.id
 INNER JOIN domain_qualified_object_names AS n
  ON o.id = n.object
`
		if len(wheres) > 0 {
			qs += "\nWHERE " + strings.Join(wheres, " AND ")
		}
		qs += fmt.Sprintf("\nORDER BY o.uuid ASC LIMIT %d", opts.Limit())
		rows, err := tx.Query(ctx, qs, qargs...)
		if err != nil {
			return errors.Internal(
				"failed reading domain-qualified object records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[domainQualifiedObjectRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting domain-qualified object records",
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

type systemQualifiedObjectRecord struct {
	ID          int64        `db:"object_id"`
	UUID        string       `db:"object_uuid"`
	Generation  int64        `db:"object_generation"`
	ObjectName  string       `db:"object_name"`
	SystemID    int64        `db:"system_id"`
	SystemUUID  string       `db:"system_uuid"`
	KindID      int64        `db:"kind_id"`
	KindName    api.KindName `db:"kind_name"`
	MetaID      int64        `db:"meta_id"`
	MetaVersion string       `db:"meta_version"`
}

// objectListSystemQualified lists zero or more Objects that have
// system-qualified names from persistent storage given the pre-validated
// expression and options.
func (s *Store) objectListSystemQualified(
	ctx context.Context,
	expr types.Expression,
	opts option.Options,
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
			v := pred.Values()[0]
			kn := v.(api.KindName)
			kindRec, err := s.kindStore.ReadByName(ctx, sys, kn)
			if err != nil {
				return nil, err
			}
			wheres = append(wheres, fmt.Sprintf("m.kind = $%d", len(qargs)+1))
			qargs = append(qargs, kindRec.RowID)
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
, o.meta AS meta_id
, m.version AS meta_version
, m.kind AS kind_id
, k.name AS kind_name
FROM objects AS o
 INNER JOIN systems AS s
  ON o.system = s.id
 INNER JOIN metas AS m
  ON o.meta = m.id
 INNER JOIN kinds AS k
  ON m.kind = k.id
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
