package store

import (
	"context"

	apikindversion "github.com/relexec/rxp/api/kindversion"
	"github.com/relexec/rxp/query"
)

// Query queries zero or more KindVersions from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr query.Expression,
	opts query.Options,
) ([]*apikindversion.KindVersion, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
