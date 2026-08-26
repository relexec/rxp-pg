package store

import (
	"context"

	apiquery "github.com/relexec/rxp/api/query"
	apisystem "github.com/relexec/rxp/api/system"
)

// Query queries zero or more Systems from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr apiquery.Expression,
	opts apiquery.Options,
) ([]*apisystem.System, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
