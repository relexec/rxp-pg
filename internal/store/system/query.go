package store

import (
	"context"

	rxpquery "github.com/relexec/rxp/query"
	rxpsystem "github.com/relexec/rxp/system"
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
