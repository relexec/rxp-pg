package store

import (
	"context"
	"errors"
	"slices"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/cache"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Store facilitates reading and writing Kind data.
type Store struct {
	// log is the top-level logger for the Store.
	log *logr.Logger
	// cfg contains the configuration options for the Store.
	cfg *config.Config
	// pool holds the underlying pgx connection pool.
	pool *pgxpool.Pool
	// byUUID is a cache that stores known Kinds keyed by kind UUID.
	byUUID *cache.Cache[byUUIDCacheKey, *Record]
	// byName is a cache that stores a lookup map of System UUID+KindName to
	// Kind UUID.
	byName *cache.Cache[byNameCacheKey, byUUIDCacheKey]
	// byRowID is a cache that stores a lookup map of Kind UUID to internal
	// DB Row ID.
	byRowID *cache.Cache[byRowIDCacheKey, byUUIDCacheKey]

	// hostSystemRecord is the host System managed by the Driver.
	hostSystemRecord storesystem.Record
	// systemStore contains the Store for reading and writing System data.
	systemStore *storesystem.Store

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
