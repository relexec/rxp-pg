package store

import (
	"context"

	apidomain "github.com/relexec/rxp/api/domain"
	apiquery "github.com/relexec/rxp/api/query"
)

// Query queries zero or more Domains from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr apiquery.Expression,
	opts apiquery.Options,
) ([]*apidomain.Domain, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
