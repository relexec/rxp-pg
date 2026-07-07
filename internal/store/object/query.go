package store

import (
	"context"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/query"

	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Query queries zero or more Objects from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	kv api.KindVersionName,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	expr query.Expression,
	opts query.Options,
) ([]*Record, error) {
	if kindRec.Kind.Scope() == api.ScopeDomain {
		return s.dbReadDomainQualifiedByExpression(
			ctx, kv, sysRec, kindRec, expr, opts,
		)
	}
	return s.dbReadSystemQualifiedByExpression(
		ctx, kv, sysRec, kindRec, expr, opts,
	)
}
