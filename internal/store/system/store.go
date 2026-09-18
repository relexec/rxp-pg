package store

import (
	"sync"

	"github.com/relexec/rxp"

	"github.com/relexec/rxp-pg/internal/cache"
	"github.com/relexec/rxp-pg/internal/store"
)

// Store facilitates reading and writing System data.
type Store struct {
	store.Store

	// cacheLock protects the set of lookup caches.
	cacheLock sync.RWMutex
	// byUUID is a cache that stores known Systems, keyed by system identifier.
	byUUID *cache.Cache[byUUIDCacheKey, *rxp.System]
	// byRowID is a cache that stores a lookup map of System UUID to internal
	// DB Row ID.
	byRowID *cache.Cache[byRowIDCacheKey, byUUIDCacheKey]
}
