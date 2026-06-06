package store

import (
	"context"
	"errors"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store facilitates reading and writing data to the database.
type Store struct {
	// pool holds the underlying pgx connection pool.
	pool *pgxpool.Pool

	// onClose are a set of callbacks that will be executed in reverse
	// order when the Store is closed.
	onClose []func(context.Context) error
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
