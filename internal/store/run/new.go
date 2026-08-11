package store

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/store"
	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storekindversion "github.com/relexec/rxp-pg/internal/store/kindversion"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
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

// WithHostSystemRecord sets the Store's host system record to the supplied
// value.
func WithHostSystemRecord(rec storesystem.Record) WithOption {
	return func(s *Store) {
		s.hostSystemRecord = rec
	}
}

// WithSystemStore sets the Store's system Store to the supplied value.
func WithSystemStore(store *storesystem.Store) WithOption {
	return func(s *Store) {
		s.systemStore = store
	}
}

// WithKindStore sets the Store's kind Store to the supplied value.
func WithKindStore(store *storekind.Store) WithOption {
	return func(s *Store) {
		s.kindStore = store
	}
}

// WithKindVersionStore sets the Store's kindversion Store to the supplied
// value.
func WithKindVersionStore(store *storekindversion.Store) WithOption {
	return func(s *Store) {
		s.kindversionStore = store
	}
}

// WithDomainStore sets the Store's domain Store to the supplied value.
func WithDomainStore(store *storedomain.Store) WithOption {
	return func(s *Store) {
		s.domainStore = store
	}
}
