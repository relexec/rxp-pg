package store

import (
	"github.com/relexec/rxp-pg/internal/cache"
	"github.com/relexec/rxp-pg/internal/store"
)

// Store facilitates reading and writing System data.
type Store struct {
	store.Store

	// byUUID is a cache that stores known Systems, keyed by system identifier.
	byUUID *cache.Cache[byUUIDCacheKey, *Record]
	// byRowID is a cache that stores a lookup map of System UUID to internal
	// DB Row ID.
	byRowID *cache.Cache[byRowIDCacheKey, byUUIDCacheKey]
}
