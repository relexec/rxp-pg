package store

import (
	"context"

	"github.com/relexec/rxp"
	rxpquery "github.com/relexec/rxp/query"
)

// Query queries zero or more Domains from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) ([]*rxp.Domain, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
