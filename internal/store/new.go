package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type WithOption func(*Store)

// New returns a new Store.
func New(
	ctx context.Context,
	opts ...WithOption,
) (*Store, error) {
	s := &Store{}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// WithPool sets the Store's DB connection pool to the supplied value.
func WithPool(pool *pgxpool.Pool) WithOption {
	return func(s *Store) {
		s.pool = pool
	}
}
