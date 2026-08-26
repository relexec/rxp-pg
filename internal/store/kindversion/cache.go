package store

import (
	"context"
	"fmt"
	"strings"

	apikindversion "github.com/relexec/rxp/api/kindversion"
	apisystem "github.com/relexec/rxp/api/system"
	"github.com/relexec/rxp/errors"
)

type byRowIDCacheKey int64
type byNameCacheKey string

func (k byNameCacheKey) SystemUUID() string {
	parts := strings.Split(string(k), "|")
	return parts[0]
}

func (k byNameCacheKey) KindVersion() apikindversion.Name {
	parts := strings.Split(string(k), "|")
	return apikindversion.Name(parts[1])
}

func newByNameCacheKey(
	system *apisystem.System,
	kv apikindversion.Name,
) byNameCacheKey {
	return byNameCacheKey(system.UUID + "|" + string(kv))
}

// cacheReadByRowID looks up a cached KindVersion by RowID, returning the cached
// apikindversion.KindVersion and whether or not the entry was found.
func (s *Store) cacheReadByRowID(
	ctx context.Context,
	key byRowIDCacheKey,
) (*apikindversion.KindVersion, bool) {
	if s.byRowID == nil {
		return nil, false
	}

	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	kv, found := s.byRowID.Get(key)
	if !found {
		return nil, false
	}
	return s.cacheReadByNameNoLock(ctx, kv)
}

// cacheReadByName looks up a cached KindVersion by Name, returning
// the cached apikindversion.KindVersion and whether or not the entry was found.
func (s *Store) cacheReadByName(
	ctx context.Context,
	key byNameCacheKey,
) (*apikindversion.KindVersion, bool) {
	if s.byName == nil {
		return nil, false
	}

	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	return s.cacheReadByNameNoLock(ctx, key)
}

// cacheReadByNameNoLock looks up a cached Kind by name, returning the cached
// apikindversion.KindVersion and whether or not the entry was found. This method assumes the cache
// lock is already held.
func (s *Store) cacheReadByNameNoLock(
	ctx context.Context,
	key byNameCacheKey,
) (*apikindversion.KindVersion, bool) {
	return s.byName.Get(key)
}

// cacheWrite ensures the supplied apikindversion.KindVersion is written to the lookup caches if
// enabled.
func (s *Store) cacheWrite(
	ctx context.Context,
	rec *apikindversion.KindVersion,
) error {
	if s.byName == nil {
		return nil
	}

	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	nameKey := newByNameCacheKey(
		rec.System,
		rec.Name(),
	)
	set := s.byName.Set(nameKey, rec)
	if !set {
		return errors.Internal(
			fmt.Sprintf(
				"failed setting kindversion cache name key %q", nameKey,
			),
		)
	}
	// Here we populate our row ID -> kv map
	rowID := rec.SystemInternalIDInt64()
	rowIDKey := byRowIDCacheKey(rowID)
	set = s.byRowID.Set(rowIDKey, nameKey)
	if !set {
		return errors.Internal(
			fmt.Sprintf(
				"failed setting kindversion cache rowid key %d", rowIDKey,
			),
		)
	}
	return nil
}
