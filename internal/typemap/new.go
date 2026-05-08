package typemap

import (
	"context"

	"github.com/relexec/rxp-pg/config"
)

// WithOption allows configuring the returned TypeMap.
type WithOption func(*TypeMap)

// New returns a new TypeMap.
func New(
	ctx context.Context,
	opts ...WithOption,
) (*TypeMap, error) {
	m := &TypeMap{}
	for _, opt := range opts {
		opt(m)
	}
	if err := m.init(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

// WithConfig sets the TypeMap's configuration.
func WithConfig(cfg config.CacheConfig) WithOption {
	return func(m *TypeMap) {
		m.cfg = cfg
	}
}
