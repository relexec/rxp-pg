package store

import (
	"context"

	rxpquery "github.com/relexec/rxp/api/query"
	rxprun "github.com/relexec/rxp/api/run"
)

// Query queries zero or more Runs from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) ([]*rxprun.Run, error) {
	return s.dbReadByExpression(ctx, expr, opts)
}
