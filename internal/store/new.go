package store

import (
	"context"

	"github.com/go-logr/logr"
	rxptypes "github.com/relexec/rxp/types"

	"github.com/relexec/rxp-pg/config"
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

// WithHostSystemUUID sets the Store's host system UUID.
func WithHostSystemUUID(uuid string) WithOption {
	return func(s *Store) {
		s.hostSystemUUID = uuid
	}
}

// WithHostSystemName sets the Store's host system name.
func WithHostSystemName(name string) WithOption {
	return func(s *Store) {
		s.hostSystemName = name
	}
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

// WithMetrics sets the Store's Metrics handler to the supplied value.
func WithMetrics(metrics rxptypes.Metrics) WithOption {
	return func(s *Store) {
		s.metrics = metrics
	}
}
