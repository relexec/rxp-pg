package cache

import (
	"context"

	"github.com/dgraph-io/ristretto/v2"

	"github.com/relexec/rxp-pg/config"
)

// WithOption allows configuring the returned Cache.
type WithOption[K ristretto.Key, V any] func(*Cache[K, V])

// New returns a new Cache.
func New[K ristretto.Key, V any](
	ctx context.Context,
	opts ...WithOption[K, V],
) (*Cache[K, V], error) {
	l := &Cache[K, V]{}
	for _, opt := range opts {
		opt(l)
	}
	if err := l.init(ctx); err != nil {
		return nil, err
	}
	return l, nil
}

// WithType sets the Cache's Type.
func WithType[K ristretto.Key, V any](typ string) WithOption[K, V] {
	return func(c *Cache[K, V]) {
		c.typ = typ
	}
}

// WithConfig sets the Cache's configuration.
func WithConfig[K ristretto.Key, V any](cfg config.CacheConfig) WithOption[K, V] {
	return func(c *Cache[K, V]) {
		c.cfg = cfg
	}
}
