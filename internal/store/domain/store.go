package store

import (
	"github.com/relexec/rxp-pg/internal/cache"
	"github.com/relexec/rxp-pg/internal/store"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Store facilitates reading and writing Domain data.
type Store struct {
	store.Store

	// byUUID is a cache that stores known Domains keyed by domain UUID.
	byUUID *cache.Cache[byUUIDCacheKey, *Record]
	// byName is a cache that stores a lookup map of System UUID+DomainName to
	// Domain UUID.
	byName *cache.Cache[byNameCacheKey, byUUIDCacheKey]
	// byRowID is a cache that stores a lookup map of Domain UUID to internal
	// DB Row ID.
	byRowID *cache.Cache[byRowIDCacheKey, byUUIDCacheKey]

	// hostSystemRecord is the host System managed by the Driver.
	hostSystemRecord storesystem.Record
	// systemStore contains the Store for reading and writing System data.
	systemStore *storesystem.Store
}
