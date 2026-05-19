package store

import (
	"context"
	"errors"
	"slices"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/cache"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Store facilitates reading and writing Meta data.
type Store struct {
	// log is the top-level logger for the Store.
	log *logr.Logger
	// cfg contains the configuration options for the Store.
	cfg *config.Config
	// pool holds the underlying pgx connection pool.
	pool *pgxpool.Pool
	// byKindVersion is a cache that stores known Metas keyed by System
	// UUID+KindVersion..
	byKindVersion *cache.Cache[byKindVersionCacheKey, *Record]
	// byRowID is a cache that stores a lookup map of Meta System
	// UUID+KindVersion to internal DB Row ID.
	byRowID *cache.Cache[byRowIDCacheKey, byKindVersionCacheKey]

	// hostSystemRecord is the host System managed by the Driver.
	hostSystemRecord storesystem.Record
	// systemStore contains the Store for reading and writing System data.
	systemStore *storesystem.Store
	// kindStore contains the Store for reading and writing Kind data.
	kindStore *storekind.Store

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
