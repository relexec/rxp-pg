package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/jackc/pgx/v5"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/expression"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/namespace"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/object/list"
	"github.com/relexec/rxp/object/list/option"
	"github.com/relexec/rxp/object/list/result"
	"github.com/relexec/rxp/system"
	rxptypes "github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	DefaultObjectListLimit = 10
	MaxObjectListLimit     = 100
)

// ObjectLister lists zero or more Objects from persistent storage.
func (s *Store) ObjectList(
	ctx context.Context,
	expr rxptypes.Expression,
	opts ...option.Option,
) (list.Result, error) {
	err := s.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeTargetType(metrics.TargetTypeObject),
		}
		if err != nil {
			attrs = append(attrs, metrics.AttributeErrCode(err))
		}
		metrics.InstrumentListRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentListDuration.Record(ctx, elapsed)
	}()

	lopts := option.New(opts...)
	err = s.objectListValidate(ctx, expr, lopts)
	if err != nil {
		return nil, err
	}

	boundedOpts := s.objectListBoundedOptions(ctx, lopts)

	entries, err := s.objectList(
		ctx, expr, boundedOpts,
	)
	if err != nil {
		return nil, err
	}
	objs := make([]rxptypes.Object, 0, len(entries))
	for _, entry := range entries {
		objs = append(objs, entry.Object)
	}
	resOpts := option.New(
		option.WithLimit(boundedOpts.Limit()),
	)
	if len(entries) == boundedOpts.Limit() {
		resOpts = option.New(
			option.WithMarker(entries[len(entries)-1].Object.UUID()),
			option.WithLimit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []result.Option{
		result.WithObjects(objs),
		result.WithOptions(resOpts),
	}
	return result.New(resNewOpts...), nil
}

// objectListValidate returns an error if the supplied expression and list
// options are not valid.
func (s *Store) objectListValidate(
	ctx context.Context,
	expr rxptypes.Expression,
	opts option.Options,
) error {
	if expr == nil {
		return errors.ErrListExpressionRequired
	}
	if !expression.ContainsKindPredicate(expr) {
		return errors.ErrInvalidListExpressionKindRequired
	}
	return nil
}

// objectListBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records listed is less
// than the max page result.
func (s *Store) objectListBoundedOptions(
	ctx context.Context,
	opts option.Options,
) option.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultObjectListLimit
	}
	limit = min(limit, MaxObjectListLimit)
	return option.New(option.WithLimit(limit))
}

// objectList lists zero or more Objects from persistent storage given the
// pre-validated expression and options.
func (s *Store) objectList(
	ctx context.Context,
	expr rxptypes.Expression,
	opts option.Options,
) ([]*objectEntry, error) {
	var err error
	var nsqRecords []namespaceQualifiedObjectRecord
	var dqRecords []domainQualifiedObjectRecord
	var sqRecords []systemQualifiedObjectRecord

	systemEntry := s.hostSystem

	var nsqExpr rxptypes.Expression
	var dqExpr rxptypes.Expression
	var sqExpr rxptypes.Expression

	switch expr := expr.(type) {
	case expression.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case expression.KindNamePredicate:
			v := pred.Values()[0]
			kn := v.(rxptypes.KindName)
			kindEntry, err := s.kindRead(ctx, systemEntry, kn)
			if err != nil {
				return nil, err
			}
			namescope := kindEntry.Kind.Namescope()
			switch namescope {
			case rxptypes.NamescopeNamespace:
				nsqExpr = expr
			case rxptypes.NamescopeDomain:
				dqExpr = expr
			default:
				sqExpr = expr
			}
		}
	case expression.OrExpression:
	case expression.AndExpression:
	}

	out := []*objectEntry{}
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
			kv := rxptypes.NewKindVersion(rec.KindName, *sv)
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
			entry := &objectEntry{
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
			kv := rxptypes.NewKindVersion(rec.KindName, *sv)
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
			entry := &objectEntry{
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
			kv := rxptypes.NewKindVersion(rec.KindName, *sv)
			sys := system.New(system.WithUUID(rec.SystemUUID))
			obj := object.New(
				object.WithSystem(sys),
				object.WithKindVersion(kv),
			)
			entry := &objectEntry{
				RowID:  rec.ID,
				Object: obj,
			}
			out = append(out, entry)
		}
	}

	if len(out) > opts.Limit() {
		// We may have more than the limit because we read
		// differently-qualified object records with the same limit.
		slices.SortFunc(out, func(a, b *objectEntry) int {
			return strings.Compare(a.Object.UUID(), b.Object.UUID())
		})
		out = out[0:opts.Limit()]
	}
	return out, nil
}

type namespaceQualifiedObjectRecord struct {
	ID            int64                  `db:"object_id"`
	UUID          string                 `db:"object_uuid"`
	Generation    int64                  `db:"object_generation"`
	ObjectName    string                 `db:"object_name"`
	SystemID      int64                  `db:"system_id"`
	SystemUUID    string                 `db:"system_uuid"`
	KindID        int64                  `db:"kind_id"`
	KindName      rxptypes.KindName      `db:"kind_name"`
	MetaID        int64                  `db:"meta_id"`
	MetaVersion   string                 `db:"meta_version"`
	DomainID      int64                  `db:"domain_id"`
	DomainName    rxptypes.DomainName    `db:"domain_name"`
	NamespaceID   int64                  `db:"namespace_id"`
	NamespaceName rxptypes.NamespaceName `db:"namespace_name"`
}

// objectListNamespaceQualified lists zero or more Objects that have
// namespace-qualified names from persistent storage given the pre-validated
// expression and options.
func (s *Store) objectListNamespaceQualified(
	ctx context.Context,
	expr rxptypes.Expression,
	opts option.Options,
) ([]namespaceQualifiedObjectRecord, error) {
	systemEntry := s.hostSystem

	qargs := []any{
		systemEntry.RowID,
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
			kn := v.(rxptypes.KindName)
			kindEntry, err := s.kindRead(ctx, systemEntry, kn)
			if err != nil {
				return nil, err
			}
			wheres = append(wheres, fmt.Sprintf("m.kind = $%d", len(qargs)+1))
			qargs = append(qargs, kindEntry.RowID)
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
	ID          int64               `db:"object_id"`
	UUID        string              `db:"object_uuid"`
	Generation  int64               `db:"object_generation"`
	ObjectName  string              `db:"object_name"`
	SystemID    int64               `db:"system_id"`
	SystemUUID  string              `db:"system_uuid"`
	KindID      int64               `db:"kind_id"`
	KindName    rxptypes.KindName   `db:"kind_name"`
	MetaID      int64               `db:"meta_id"`
	MetaVersion string              `db:"meta_version"`
	DomainID    int64               `db:"domain_id"`
	DomainName  rxptypes.DomainName `db:"domain_name"`
}

// objectListDomainQualified lists zero or more Objects that have
// domain-qualified names from persistent storage given the pre-validated
// expression and options.
func (s *Store) objectListDomainQualified(
	ctx context.Context,
	expr rxptypes.Expression,
	opts option.Options,
) ([]domainQualifiedObjectRecord, error) {
	systemEntry := s.hostSystem

	qargs := []any{
		systemEntry.RowID,
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
			kn := v.(rxptypes.KindName)
			kindEntry, err := s.kindRead(ctx, systemEntry, kn)
			if err != nil {
				return nil, err
			}
			wheres = append(wheres, fmt.Sprintf("m.kind = $%d", len(qargs)+1))
			qargs = append(qargs, kindEntry.RowID)
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
	ID          int64             `db:"object_id"`
	UUID        string            `db:"object_uuid"`
	Generation  int64             `db:"object_generation"`
	ObjectName  string            `db:"object_name"`
	SystemID    int64             `db:"system_id"`
	SystemUUID  string            `db:"system_uuid"`
	KindID      int64             `db:"kind_id"`
	KindName    rxptypes.KindName `db:"kind_name"`
	MetaID      int64             `db:"meta_id"`
	MetaVersion string            `db:"meta_version"`
}

// objectListSystemQualified lists zero or more Objects that have
// system-qualified names from persistent storage given the pre-validated
// expression and options.
func (s *Store) objectListSystemQualified(
	ctx context.Context,
	expr rxptypes.Expression,
	opts option.Options,
) ([]systemQualifiedObjectRecord, error) {
	systemEntry := s.hostSystem

	qargs := []any{
		systemEntry.RowID,
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
			kn := v.(rxptypes.KindName)
			kindEntry, err := s.kindRead(ctx, systemEntry, kn)
			if err != nil {
				return nil, err
			}
			wheres = append(wheres, fmt.Sprintf("m.kind = $%d", len(qargs)+1))
			qargs = append(qargs, kindEntry.RowID)
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

var _ list.ObjectLister = (*Store)(nil)
