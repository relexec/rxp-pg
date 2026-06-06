package store

import (
	"github.com/go-logr/logr"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/cache"
	"github.com/relexec/rxp-pg/internal/store"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Store facilitates reading and writing KindVersion data.
type Store struct {
	store.Store

	// log is the top-level logger for the Store.
	log *logr.Logger
	// cfg contains the configuration options for the Store.
	cfg *config.Config

	// byKindVersion is a cache that stores known KindVersions keyed by System
	// UUID+KindVersionName..
	byName *cache.Cache[byNameCacheKey, *Record]
	// byRowID is a cache that stores a lookup map of System UUID +
	// KindVersionName to internal DB Row ID.
	byRowID *cache.Cache[byRowIDCacheKey, byNameCacheKey]

	// hostSystemRecord is the host System managed by the Driver.
	hostSystemRecord storesystem.Record
	// systemStore contains the Store for reading and writing System data.
	systemStore *storesystem.Store
	// kindStore contains the Store for reading and writing Kind data.
	kindStore *storekind.Store
}
