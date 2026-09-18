package store

import (
	"context"

	"github.com/relexec/rxp"
	rxpquery "github.com/relexec/rxp/query"
)

// Query queries zero or more Objects from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	kv rxp.KindVersionName,
	sysRec *rxp.System,
	kindRec *rxp.Kind,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) ([]*Record, error) {
	if kindRec.Scope == rxp.ScopeDomain {
		return s.dbReadDomainQualifiedByExpression(
			ctx, kv, sysRec, kindRec, expr, opts,
		)
	}
	return s.dbReadSystemQualifiedByExpression(
		ctx, kv, sysRec, kindRec, expr, opts,
	)
}
