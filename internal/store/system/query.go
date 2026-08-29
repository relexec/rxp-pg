package store

import (
	"context"

	rxpquery "github.com/relexec/rxp/api/query"
	rxpsystem "github.com/relexec/rxp/api/system"
)

// Query queries zero or more Systems from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) ([]*rxpsystem.System, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
