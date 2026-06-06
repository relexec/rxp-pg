package store

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/relexec/rxp-pg/config"
	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storekindversion "github.com/relexec/rxp-pg/internal/store/kindversion"
	storenamespace "github.com/relexec/rxp-pg/internal/store/namespace"
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
		s.SetPool(pool)
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

// WithNamespaceStore sets the Store's namespace Store to the supplied value.
func WithNamespaceStore(store *storenamespace.Store) WithOption {
	return func(s *Store) {
		s.namespaceStore = store
	}
}
