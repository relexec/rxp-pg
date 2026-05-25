package store

import (
	"context"
	"errors"
	"slices"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/cache"
	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Store facilitates reading and writing Namespace data.
type Store struct {
	// log is the top-level logger for the Store.
	log *logr.Logger
	// cfg contains the configuration options for the Store.
	cfg *config.Config
	// pool holds the underlying pgx connection pool.
	pool *pgxpool.Pool
	// byUUID is a cache that stores known Namespaces keyed by namespace UUID.
	byUUID *cache.Cache[byUUIDCacheKey, *Record]
	// byName is a cache that stores a lookup map of SystemUUID + DomainUUID +
	// NamespaceName to Namespace UUID.
	byName *cache.Cache[byNameCacheKey, byUUIDCacheKey]
	// byRowID is a cache that stores a lookup map of Namespace UUID to
	// internal DB Row ID.
	byRowID *cache.Cache[byRowIDCacheKey, byUUIDCacheKey]

	// hostSystemRecord is the host System managed by the Driver.
	hostSystemRecord storesystem.Record
	// domainStore contains the Store for reading and writing Domain data.
	domainStore *storedomain.Store

	// onClose are a set of callbacks that will be executed in reverse
	// order when the Store is closed.
	onClose []func(context.Context) error
}

// Close tears down the Store and executes any callbacks that were registered
// to execute on shutdown.
func (s *Store) Close(ctx context.Context) error {
	var err error
	slices.Reverse(s.onClose)
	for _, cb := range s.onClose {
		err = errors.Join(err, cb(ctx))
	}
	return err
}
