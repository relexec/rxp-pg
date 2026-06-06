package store

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/relexec/rxp-pg/config"
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
	if err := s.init(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// WithConfig sets the Store's Config to the supplied value.
func WithConfig(cfg *config.Config) WithOption {
	return func(s *Store) {
		s.SetConfig(cfg)
	}
}

// WithLogger sets the Store's Logger to the supplied value.
func WithLogger(logger logr.Logger) WithOption {
	return func(s *Store) {
		s.SetLogger(logger)
	}
}

// WithPool sets the Store's DB connection pool to the supplied value.
func WithPool(pool *pgxpool.Pool) WithOption {
	return func(s *Store) {
		s.SetPool(pool)
	}
}
