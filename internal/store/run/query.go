package store

import (
	"context"

	apirun "github.com/relexec/rxp/api/run"
	"github.com/relexec/rxp/query"
)

// Query queries zero or more Runs from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr query.Expression,
	opts query.Options,
) ([]*apirun.Run, error) {
	return s.dbReadByExpression(ctx, expr, opts)
}
