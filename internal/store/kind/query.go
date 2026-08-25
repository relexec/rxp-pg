package store

import (
	"context"

	apikind "github.com/relexec/rxp/api/kind"
	"github.com/relexec/rxp/query"
)

// Query queries zero or more Kinds from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr query.Expression,
	opts query.Options,
) ([]*apikind.Kind, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
