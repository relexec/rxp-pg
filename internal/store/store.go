package store

import (
	"context"
	"errors"
	"slices"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/relexec/rxp-pg/config"
)

// Store facilitates reading and writing data to the database.
type Store struct {
	// logher is the top-level logger for the Store.
	logger logr.Logger
	// cfg contains the configuration options for the Store.
	cfg *config.Config
	// pool holds the underlying pgx connection pool.
	pool *pgxpool.Pool

	// onClose are a set of callbacks that will be executed in reverse
	// order when the Store is closed.
	onClose []func(context.Context) error
}

// Config returns the Store's config options.
func (s Store) Config() *config.Config {
	return s.cfg
}

// SetConfig sets the Store's config options.
func (s *Store) SetConfig(cfg *config.Config) {
	s.cfg = cfg
}

// SetLogger sets the Store's logger.
func (s *Store) SetLogger(logger logr.Logger) {
	s.logger = logger
}

// Logger returns the Store's logger.
func (s Store) Logger() logr.Logger {
	return s.logger
}

// Debug logs the supplied message and optional k/v pairs at DEBUG level.
func (s Store) Debug(msg string, keysAndValues ...any) {
	s.logger.V(4).Info(msg, keysAndValues...)
}

// Info logs the supplied message and optional k/v pairs at INFO level.
func (s Store) Info(msg string, keysAndValues ...any) {
	s.logger.Info(msg, keysAndValues...)
}

// SetPool sets the Store's connection pool.
func (s *Store) SetPool(pool *pgxpool.Pool) {
	s.pool = pool
}

// OnClose registers a callback that will be executed when `Close` is called.
func (s *Store) OnClose(cb func(context.Context) error) {
	s.onClose = append(s.onClose, cb)
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
