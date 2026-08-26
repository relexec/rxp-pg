package store

import (
	"context"

	apiquery "github.com/relexec/rxp/api/query"
	apirun "github.com/relexec/rxp/api/run"
)

// Query queries zero or more Runs from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr apiquery.Expression,
	opts apiquery.Options,
) ([]*apirun.Run, error) {
	return s.dbReadByExpression(ctx, expr, opts)
}
