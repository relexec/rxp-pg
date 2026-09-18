package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/relexec/rxp"
	rxperrors "github.com/relexec/rxp/errors"
)

type byRowIDCacheKey int64
type byUUIDCacheKey string
type byNameCacheKey string

func (k byNameCacheKey) SystemUUID() string {
	parts := strings.Split(string(k), "|")
	return parts[0]
}

func (k byNameCacheKey) KindName() rxp.KindName {
	parts := strings.Split(string(k), "|")
	return rxp.KindName(parts[1])
}

func newByNameCacheKey(
	system *rxp.System,
	name rxp.KindName,
) byNameCacheKey {
	return byNameCacheKey(system.UUID + "|" + string(name))
}

// cacheReadByRowID looks up a cached Kind by RowID, returning the cached
// rxp.Kind and whether or not the entry was found.
func (s *Store) cacheReadByRowID(
	ctx context.Context,
	key byRowIDCacheKey,
) (*rxp.Kind, bool) {
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

// cacheReadByUUID looks up a cached Domain by UUID, returning the cached
// rxp.Kind and whether or not the entry was found.
func (s *Store) cacheReadByUUID(
	ctx context.Context,
	key byUUIDCacheKey,
) (*rxp.Kind, bool) {
	if s.byUUID == nil {
		return nil, false
	}

	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	return s.cacheReadByUUIDNoLock(ctx, key)
}

// cacheReadByUUIDNoLock looks up a cached Kind by UUID, returning the cached
// rxp.Kind and whether or not the entry was found. This method assumes the cache
// lock is already held.
func (s *Store) cacheReadByUUIDNoLock(
	ctx context.Context,
	key byUUIDCacheKey,
) (*rxp.Kind, bool) {
	return s.byUUID.Get(key)
}

// cacheReadByName looks up a cached Kind by name, returning the cached rxp.Kind
// and whether or not the entry was found.
func (s *Store) cacheReadByName(
	ctx context.Context,
	key byNameCacheKey,
) (*rxp.Kind, bool) {
	if s.byName == nil {
		return nil, false
	}

	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	uuid, found := s.byName.Get(key)
	if !found {
		return nil, false
	}
	return s.cacheReadByUUIDNoLock(ctx, uuid)
}

// cacheWrite ensures the supplied rxp.Kind is written to the lookup caches if
// enabled.
func (s *Store) cacheWrite(
	ctx context.Context,
	rec *rxp.Kind,
) error {
	if s.byUUID == nil {
		return nil
	}

	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	uuidKey := byUUIDCacheKey(rec.UUID)
	set := s.byUUID.Set(uuidKey, rec)
	if !set {
		return rxperrors.Internal(
			fmt.Sprintf("failed setting kind cache uuid key %q", uuidKey),
		)
	}
	// Here we populate our row ID -> uuid and name -> uuid maps
	nameKey := newByNameCacheKey(rec.System, rec.Name)
	uuid := rec.UUID
	set = s.byName.Set(nameKey, byUUIDCacheKey(uuid))
	if !set {
		return rxperrors.Internal(
			fmt.Sprintf("failed setting kind cache name key %q", nameKey),
		)
	}
	rowID := rec.SystemInternalIDInt64()
	rowIDKey := byRowIDCacheKey(rowID)
	set = s.byRowID.Set(rowIDKey, byUUIDCacheKey(uuid))
	if !set {
		return rxperrors.Internal(
			fmt.Sprintf("failed setting kind cache rowid key %d", rowIDKey),
		)
	}
	return nil
}
