package store

import (
	"context"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/query"
)

// Query queries zero or more Objects from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	kv api.KindVersionName,
	sysRec *api.System,
	kindRec *api.Kind,
	expr query.Expression,
	opts query.Options,
) ([]*Record, error) {
	if kindRec.Scope == api.ScopeDomain {
		return s.dbReadDomainQualifiedByExpression(
			ctx, kv, sysRec, kindRec, expr, opts,
		)
	}
	return s.dbReadSystemQualifiedByExpression(
		ctx, kv, sysRec, kindRec, expr, opts,
	)
}
