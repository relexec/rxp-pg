package store

import (
	"context"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"
)

// Query queries zero or more Objects from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	kv api.KindVersionName,
	expr expression.Expression,
	opts query.Options,
) ([]*Record, error) {

	sys := s.hostSystemRecord.System
	kn := kv.Kind()
	kindRec, err := s.kindStore.ReadByName(ctx, sys, kn)
	if err != nil {
		return nil, err
	}

	scope := kindRec.Kind.Scope()
	switch scope {
	case api.ScopeNamespace:
		return s.dbReadNamespaceQualifiedByExpression(
			ctx, kv, kindRec, expr, opts,
		)
	case api.ScopeDomain:
		return s.dbReadDomainQualifiedByExpression(
			ctx, kv, kindRec, expr, opts,
		)
	default:
		return s.dbReadSystemQualifiedByExpression(
			ctx, kv, kindRec, expr, opts,
		)
	}
}
