package store

import (
	"context"

	apicore "github.com/relexec/rxp/api/core"
	apikind "github.com/relexec/rxp/api/kind"
	apikindversion "github.com/relexec/rxp/api/kindversion"
	apiquery "github.com/relexec/rxp/api/query"
	apisystem "github.com/relexec/rxp/api/system"
)

// Query queries zero or more Objects from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	kv apikindversion.Name,
	sysRec *apisystem.System,
	kindRec *apikind.Kind,
	expr apiquery.Expression,
	opts apiquery.Options,
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
