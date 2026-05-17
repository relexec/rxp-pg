package store

import (
	"context"
	"errors"
	"slices"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/cache"
)

// Store facilitates reading and writing System data.
type Store struct {
	// log is the top-level logger for the Store.
	log *logr.Logger
	// cfg contains the configuration options for the Store.
	cfg *config.Config
	// pool holds the underlying pgx connection pool.
	pool *pgxpool.Pool
	// byUUID is a cache that stores known Systems, keyed by system identifier.
	byUUID *cache.Cache[byUUIDCacheKey, *Record]

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
