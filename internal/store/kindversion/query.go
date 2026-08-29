package store

import (
	"context"

	rxpkindversion "github.com/relexec/rxp/api/kindversion"
	rxpquery "github.com/relexec/rxp/api/query"
)

// Query queries zero or more KindVersions from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr rxpquery.Expression,
	opts rxpquery.Options,
) ([]*rxpkindversion.KindVersion, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
