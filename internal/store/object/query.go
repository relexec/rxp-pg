package store

import (
	"context"
	"slices"
	"strings"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"
)

// Query queries zero or more Objects from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr expression.Expression,
	opts query.Options,
) ([]*Record, error) {

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
		sqRecords, err := s.dbReadSystemQualifiedByExpression(
			ctx, sqExpr, opts,
		)
		if err != nil {
			return nil, err
		}
		for _, rec := range sqRecords {
			out = append(out, rec)
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
