package store

import (
	"context"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/query"
)

// Query queries zero or more KindVersions from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr query.Expression,
	opts query.Options,
) ([]*api.KindVersion, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
