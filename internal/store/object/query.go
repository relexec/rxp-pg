package store

import (
	"context"

	apicore "github.com/relexec/rxp/api/core"
	rxpkind "github.com/relexec/rxp/api/kind"
	rxpkindversion "github.com/relexec/rxp/api/kindversion"
	rxpquery "github.com/relexec/rxp/api/query"
	rxpsystem "github.com/relexec/rxp/api/system"
)

// Query queries zero or more Objects from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	kv rxpkindversion.Name,
	sysRec *rxpsystem.System,
	kindRec *rxpkind.Kind,
	expr rxpquery.Expression,
	opts rxpquery.Options,
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
