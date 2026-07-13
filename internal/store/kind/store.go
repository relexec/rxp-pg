package store

import (
	"sync"

	"github.com/relexec/rxp-pg/internal/cache"
	"github.com/relexec/rxp-pg/internal/store"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Store facilitates reading and writing Kind data.
type Store struct {
	store.Store

	// cacheLock protects the set of lookup caches.
	cacheLock sync.RWMutex
	// byUUID is a cache that stores known Kinds keyed by kind UUID.
	byUUID *cache.Cache[byUUIDCacheKey, *Record]
	// byName is a cache that stores a lookup map of System UUID+KindName to
	// Kind UUID.
	byName *cache.Cache[byNameCacheKey, byUUIDCacheKey]
	// byRowID is a cache that stores a lookup map of Kind UUID to internal
	// DB Row ID.
	byRowID *cache.Cache[byRowIDCacheKey, byUUIDCacheKey]

	// hostSystemRecord is the host System managed by the Driver.
	hostSystemRecord storesystem.Record
	// systemStore contains the Store for reading and writing System data.
	systemStore *storesystem.Store
}
