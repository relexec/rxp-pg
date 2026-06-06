package store

import (
	"github.com/go-logr/logr"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/cache"
	"github.com/relexec/rxp-pg/internal/store"
)

// Store facilitates reading and writing System data.
type Store struct {
	store.Store

	// log is the top-level logger for the Store.
	log *logr.Logger
	// cfg contains the configuration options for the Store.
	cfg *config.Config

	// byUUID is a cache that stores known Systems, keyed by system identifier.
	byUUID *cache.Cache[byUUIDCacheKey, *Record]
	// byRowID is a cache that stores a lookup map of System UUID to internal
	// DB Row ID.
	byRowID *cache.Cache[byRowIDCacheKey, byUUIDCacheKey]
}
