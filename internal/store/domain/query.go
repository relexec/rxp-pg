package store

import (
	"context"

	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"
)

// Query queries zero or more Domains from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr expression.Expression,
	opts query.Options,
) ([]*Record, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
