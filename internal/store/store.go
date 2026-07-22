package store

import (
	"context"
	"errors"
	"log/slog"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/relexec/rxp-pg/config"
)

// Store facilitates reading and writing data to the database.
type Store struct {
	// Logger is the top-level logger for the Store.
	Logger *slog.Logger
	// cfg contains the configuration options for the Store.
	Config config.Config
	// pool holds the underlying pgx connection pool.
	Pool *pgxpool.Pool

	// onClose are a set of callbacks that will be executed in reverse
	// order when the Store is closed.
	onClose []func(context.Context) error
}

// OnClose registers a callback that will be executed when `Close` is called.
func (s *Store) OnClose(cb func(context.Context) error) {
	s.onClose = append(s.onClose, cb)
}

// Close tears down the Store and executes any callbacks that were registered
// to execute on shutdown.
func (s Store) Close(ctx context.Context) error {
	var err error
	slices.Reverse(s.onClose)
	for _, cb := range s.onClose {
		err = errors.Join(err, cb(ctx))
	}
	return err
}
