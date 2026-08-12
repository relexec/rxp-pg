package store

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/store"
)

type WithOption func(*Store)

// New returns a new Store.
func New(
	ctx context.Context,
	cfg config.Config,
	pool *pgxpool.Pool,
	opts ...WithOption,
) (*Store, error) {
	s := &Store{
		Store: store.Store{
			Config: cfg,
			Pool:   pool,
		},
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.Logger == nil {
		s.Logger = s.Config.Log.Logger()
	}
	if err := s.init(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// WithLogger sets the Store's Logger to the supplied value.
func WithLogger(logger *slog.Logger) WithOption {
	return func(s *Store) {
		s.Logger = logger
	}
}
