package cache

import (
	"context"
	"unsafe"

	"github.com/dgraph-io/ristretto/v2"

	"github.com/relexec/rxp-pg/config"
)

const (
	// Ref: https://github.com/dgraph-io/ristretto/blob/402101df6c698ed1253bb305ce9cda71bc83ad1d/cache.go#L114-L122
	defaultBufferItems = int64(64)
)

// Cache wraps a ristretto.Cache struct that manages a LRU in-memory cache of
// generic key->value lookups.
type Cache[K ristretto.Key, V any] struct {
	// type is the Type of thing stored in the Lookup.
	typ string
	// cfg stores the Lookup's configuration.
	cfg config.CacheConfig
	// cache is the cache, keyed by row ID, valued by name.
	cache *ristretto.Cache[K, V]
}

// Type returns the string name of the type of thing the Lookup stores.
func (c Cache[K, V]) Type() string {
	return c.typ
}

// Close ensures the Lookup drains properly.
func (c *Cache[K, V]) Close(ctx context.Context) error {
	if c.cache != nil {
		c.cache.Close()
	}
	return nil
}

// Get returns the value at the given Key.
func (c Cache[K, V]) Get(k K) (V, bool) {
	return c.cache.Get(k)
}

// Set sets the supplied Value entry into the Cache at the supplied Key,
// returning whether the insertion occurred.
func (c *Cache[K, V]) Set(k K, v V) bool {
	if c.cache == nil {
		return false
	}

	// Cost for all entries is the same: 1.
	inserted := c.cache.Set(k, v, 1)
	c.cache.Wait()
	return inserted
}

// init sets up the Lookup, initializing the underlying ristretto cache and
// metrics.
func (c *Cache[K, V]) init(ctx context.Context) error {
	maxSize, err := c.cfg.MaxSizeBytes()
	if err != nil {
		return err
	}
	rc, err := ristretto.NewCache(
		&ristretto.Config[K, V]{
			NumCounters: numCounters(maxSize, unsafe.Sizeof("")),
			MaxCost:     int64(maxSize),
			BufferItems: defaultBufferItems,
		},
	)
	if err != nil {
		return err
	}
	c.cache = rc
	return nil
}

// numCounters calculates the recommended NumCounters setting for the Kind
// cache, which is 10x the number of entries that will fit in the cache.
//
// Ref: https://github.com/dgraph-io/ristretto/blob/402101df6c698ed1253bb305ce9cda71bc83ad1d/cache.go#L93-L94
func numCounters(maxSize int, entrySize uintptr) int64 {
	return int64((maxSize / int(entrySize)) * 10)
}
