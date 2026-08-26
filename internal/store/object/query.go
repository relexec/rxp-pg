package store

import (
	"context"

	apicore "github.com/relexec/rxp/api/core"
	apikind "github.com/relexec/rxp/api/kind"
	apikindversion "github.com/relexec/rxp/api/kindversion"
	apisystem "github.com/relexec/rxp/api/system"
	"github.com/relexec/rxp/query"
)

// Query queries zero or more Objects from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	kv apikindversion.Name,
	sysRec *apisystem.System,
	kindRec *apikind.Kind,
	expr query.Expression,
	opts query.Options,
) ([]*Record, error) {
	if kindRec.Scope == apicore.ScopeDomain {
		return s.dbReadDomainQualifiedByExpression(
			ctx, kv, sysRec, kindRec, expr, opts,
		)
	}
	return s.dbReadSystemQualifiedByExpression(
		ctx, kv, sysRec, kindRec, expr, opts,
	)
}
