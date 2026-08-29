package store

import (
	"context"

	rxpkind "github.com/relexec/rxp/api/kind"
	rxpquery "github.com/relexec/rxp/api/query"
)

// Query queries zero or more Kinds from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) ([]*rxpkind.Kind, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
