package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/system"
)

type byRowIDCacheKey int64
type byUUIDCacheKey string
type byNameCacheKey string

func (k byNameCacheKey) SystemUUID() string {
	parts := strings.Split(string(k), "|")
	return parts[0]
}

func (k byNameCacheKey) DomainName() api.DomainName {
	parts := strings.Split(string(k), "|")
	return api.DomainName(parts[1])
}

func newByNameCacheKey(
	system *system.System,
	name api.DomainName,
) byNameCacheKey {
	return byNameCacheKey(system.UUID() + "|" + string(name))
}

// cacheReadByRowID looks up a cached Domain by RowID, returning the cached
// Record and whether or not the entry was found.
func (s *Store) cacheReadByRowID(
	ctx context.Context,
	key byRowIDCacheKey,
) (*Record, bool) {
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
// Record and whether or not the entry was found.
func (s *Store) cacheReadByUUID(
	ctx context.Context,
	key byUUIDCacheKey,
) (*Record, bool) {
	if s.byUUID == nil {
		return nil, false
	}

	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	return s.cacheReadByUUIDNoLock(ctx, key)
}

// cacheReadByUUIDNoLock looks up a cached Domain by UUID, returning the cached
// Record and whether or not the entry was found. This method assumes the cache
// lock is already held.
func (s *Store) cacheReadByUUIDNoLock(
	ctx context.Context,
	key byUUIDCacheKey,
) (*Record, bool) {
	return s.byUUID.Get(key)
}

// cacheReadByName looks up a cached Domain by System UUID + Name, returning the cached
// Record and whether or not the entry was found.
func (s *Store) cacheReadByName(
	ctx context.Context,
	key byNameCacheKey,
) (*Record, bool) {
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

// cacheWrite ensures the supplied Record is written to the lookup caches if
// enabled.
func (s *Store) cacheWrite(
	ctx context.Context,
	rec *Record,
) error {
	if s.byUUID == nil {
		return nil
	}

	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	uuidKey := byUUIDCacheKey(rec.Domain.UUID())
	set := s.byUUID.Set(uuidKey, rec)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting domain cache uuid key %q", uuidKey),
		)
	}
	// Here we populate our row ID -> uuid and name -> uuid maps
	nameKey := newByNameCacheKey(rec.Domain.System(), rec.Domain.Name())
	uuid := rec.Domain.UUID()
	set = s.byName.Set(nameKey, byUUIDCacheKey(uuid))
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting domain cache name key %q", nameKey),
		)
	}
	rowIDKey := byRowIDCacheKey(rec.RowID)
	set = s.byRowID.Set(rowIDKey, byUUIDCacheKey(uuid))
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting domain cache rowid key %q", rowIDKey),
		)
	}
	return nil
}

// cacheEvict purges the caches of all Domains in the supplied Domain's tree.
func (s *Store) cacheEvict(
	ctx context.Context,
	dom domain.Domain,
) error {
	if s.byUUID == nil {
		return nil
	}

	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	uuid := dom.UUID()
	rec, found := s.cacheReadByUUIDNoLock(ctx, byUUIDCacheKey(uuid))
	if !found {
		return nil
	}

	recsInTree, err := s.dbReadDomainsInTreeByRootRowID(ctx, rec.Root)
	if err != nil {
		return fmt.Errorf(
			"failed reading domain records in tree by root: %w", err)
	}

	for _, treeRec := range recsInTree {
		if err = s.cacheEvictNoLock(ctx, treeRec); err != nil {
			return err
		}
	}
	return nil
}

// cacheEvictNoLock purges the caches of the specific Domain. This method
// assumes the cache lock is already held.
func (s *Store) cacheEvictNoLock(
	ctx context.Context,
	rec *Record,
) error {
	uuidKey := byUUIDCacheKey(rec.Domain.UUID())
	s.byUUID.Del(uuidKey)
	// Here we populate our row ID -> uuid and name -> uuid maps
	nameKey := newByNameCacheKey(rec.Domain.System(), rec.Domain.Name())
	s.byName.Del(nameKey)
	rowIDKey := byRowIDCacheKey(rec.RowID)
	s.byRowID.Del(rowIDKey)
	return nil
}
