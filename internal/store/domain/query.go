package store

import (
	"context"

	rxpdomain "github.com/relexec/rxp/api/domain"
	rxpquery "github.com/relexec/rxp/api/query"
)

// Query queries zero or more Domains from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) ([]*rxpdomain.Domain, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
