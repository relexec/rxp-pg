package store

import (
	"context"

	apikind "github.com/relexec/rxp/api/kind"
	apiquery "github.com/relexec/rxp/api/query"
)

// Query queries zero or more Kinds from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr apiquery.Expression,
	opts apiquery.Options,
) ([]*apikind.Kind, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
