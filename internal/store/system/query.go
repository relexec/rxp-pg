package store

import (
	"context"

	apisystem "github.com/relexec/rxp/api/system"
	"github.com/relexec/rxp/query"
)

// Query queries zero or more Systems from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr query.Expression,
	opts query.Options,
) ([]*apisystem.System, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
