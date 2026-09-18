package store

import (
	"context"

	"github.com/relexec/rxp"
	rxpquery "github.com/relexec/rxp/query"
)

// Query queries zero or more Kinds from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) ([]*rxp.Kind, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
