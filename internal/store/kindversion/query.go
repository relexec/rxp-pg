package store

import (
	"context"

	apikindversion "github.com/relexec/rxp/api/kindversion"
	apiquery "github.com/relexec/rxp/api/query"
)

// Query queries zero or more KindVersions from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr apiquery.Expression,
	opts apiquery.Options,
) ([]*apikindversion.KindVersion, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
