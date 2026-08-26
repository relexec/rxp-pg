package store

import (
	"context"
	"fmt"

	apierrors "github.com/relexec/rxp/api/errors"
	apisystem "github.com/relexec/rxp/api/system"
)

type byRowIDCacheKey int64
type byUUIDCacheKey string

// cacheReadByRowID looks up a cached System by RowID, returning the cached
// Record and whether or not the entry was found.
func (s *Store) cacheReadByRowID(
	ctx context.Context,
	key byRowIDCacheKey,
) (*apisystem.System, bool) {
	if s.byRowID == nil {
		return nil, false
	}

	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	uuid, found := s.byRowID.Get(key)
	if !found {
		return nil, false
	}
	return s.cacheReadByUUIDNoLock(ctx, uuid)
}

// cacheReadByUUID looks up a cached System by UUID, returning the cached
// Record and whether or not the entry  was found.
func (s *Store) cacheReadByUUID(
	ctx context.Context,
	key byUUIDCacheKey,
) (*apisystem.System, bool) {
	if s.byUUID == nil {
		return nil, false
	}

	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	return s.cacheReadByUUIDNoLock(ctx, key)
}

// cacheReadByUUIDNoLock looks up a cached System by UUID, returning the cached
// record and whether or not the entry was found. This method assumes the cache
// lock is already held.
func (s *Store) cacheReadByUUIDNoLock(
	ctx context.Context,
	key byUUIDCacheKey,
) (*apisystem.System, bool) {
	return s.byUUID.Get(key)
}

// cacheWrite ensures the supplied record is written to the lookup caches if
// enabled.
func (s *Store) cacheWrite(
	ctx context.Context,
	rec *apisystem.System,
) error {
	if s.byUUID == nil {
		return nil
	}

	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	uuidKey := byUUIDCacheKey(rec.UUID)
	set := s.byUUID.Set(uuidKey, rec)
	if !set {
		return apierrors.Internal(
			fmt.Sprintf("failed setting system cache uuid key %q", uuidKey),
		)
	}

	// Here we populate our row ID -> uuid map
	rowID := rec.SystemInternalIDInt64()
	rowIDKey := byRowIDCacheKey(rowID)
	set = s.byRowID.Set(rowIDKey, uuidKey)
	if !set {
		return apierrors.Internal(
			fmt.Sprintf("failed setting system cache rowid key %d", rowIDKey),
		)
	}
	return nil
}
