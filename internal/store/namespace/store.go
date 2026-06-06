package store

import (
	"github.com/relexec/rxp-pg/internal/cache"
	"github.com/relexec/rxp-pg/internal/store"
	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Store facilitates reading and writing Namespace data.
type Store struct {
	store.Store

	// byUUID is a cache that stores known Namespaces keyed by namespace UUID.
	byUUID *cache.Cache[byUUIDCacheKey, *Record]
	// byName is a cache that stores a lookup map of SystemUUID + DomainUUID +
	// NamespaceName to Namespace UUID.
	byName *cache.Cache[byNameCacheKey, byUUIDCacheKey]
	// byRowID is a cache that stores a lookup map of Namespace UUID to
	// internal DB Row ID.
	byRowID *cache.Cache[byRowIDCacheKey, byUUIDCacheKey]

	// hostSystemRecord is the host System managed by the Driver.
	hostSystemRecord storesystem.Record
	// domainStore contains the Store for reading and writing Domain data.
	domainStore *storedomain.Store
}
