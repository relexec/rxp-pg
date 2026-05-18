package store

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/relexec/rxp-pg/config"
	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
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
		s.cfg = cfg
	}
}

// WithLogger sets the Store's Logger to the supplied value.
func WithLogger(logger logr.Logger) WithOption {
	return func(s *Store) {
		s.log = &logger
	}
}

// WithPool sets the Store's DB connection pool to the supplied value.
func WithPool(pool *pgxpool.Pool) WithOption {
	return func(s *Store) {
		s.pool = pool
	}
}

// WithHostSystemRecord sets the Store's host system record to the supplied
// value.
func WithHostSystemRecord(rec storesystem.Record) WithOption {
	return func(s *Store) {
		s.hostSystemRecord = rec
	}
}

// WithDomainStore sets the Store's domain Store to the supplied value.
func WithDomainStore(store *storedomain.Store) WithOption {
	return func(s *Store) {
		s.domainStore = store
	}
}
