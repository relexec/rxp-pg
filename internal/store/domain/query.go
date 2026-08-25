package store

import (
	"context"

	apidomain "github.com/relexec/rxp/api/domain"
	"github.com/relexec/rxp/query"
)

// Query queries zero or more Domains from persistent storage.
func (s *Store) Query(
	ctx context.Context,
	expr query.Expression,
	opts query.Options,
) ([]*apidomain.Domain, error) {
	return s.dbReadByExpression(
		ctx, expr, opts,
	)
}
