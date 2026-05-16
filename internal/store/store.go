package store

import (
	"context"
	"errors"
	"slices"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"
	rxptypes "github.com/relexec/rxp/types"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/cache"
)

// Store implements a backend rxp persistence store using PostgreSQL.
type Store struct {
	// log is the top-level logger for the Store.
	log *logr.Logger
	// cfg contains the configuration options for the Store.
	cfg *config.Config
	// pool holds the underlying pgx connection pool.
	pool *pgxpool.Pool
	// metrics is the metrics handler for the Store.
	metrics rxptypes.Metrics

	// hostSystemUUID is the UUID of the host System managed by this Store.
	hostSystemUUID string
	// hostSystemName is the name of the host System managed by this Store, if any.
	hostSystemName string
	// hostSystem is the host System managed by this Store.
	hostSystem *systemEntry
	// systemCache stores known Systems, keyed by system identifier.
	systemCache *cache.Cache[systemCacheKey, *systemEntry]
	// kindCache stores known Kinds, keyed by kind name.
	kindCache *cache.Cache[kindCacheKey, *kindEntry]
	// domainCache stores known Domains, keyed by Domain Name.
	domainCache *cache.Cache[domainCacheKey, *domainEntry]
	// namespaceCache stores known Namespaces, keyed by Namespace Name.
	namespaceCache *cache.Cache[namespaceCacheKey, *namespaceEntry]
	// metaCache stores the latest generation of Metas, keyed by
	// KindVersion.
	metaCache *cache.Cache[metaCacheKey, *metaEntry]

	// onClose are a set of callbacks that will be executed in reverse
	// order when the Store is closed.
	onClose []func(context.Context) error
}

// Metrics returns the Store's configured metrics handler.
func (s *Store) Metrics() rxptypes.Metrics {
	return s.metrics
}

// Close tears down the Store and executes any callbacks that were registered
// to execute on shutdown.
func (s *Store) Close(ctx context.Context) error {
	if s.pool != nil {
		s.pool.Close()
	}
	var err error
	slices.Reverse(s.onClose)
	for _, cb := range s.onClose {
		err = errors.Join(err, cb(ctx))
	}
	return err
}
