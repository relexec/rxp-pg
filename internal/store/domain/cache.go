package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/relexec/rxp/api"
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
	uuid, found := s.byRowID.Get(key)
	if !found {
		return nil, false
	}
	return s.cacheReadByUUID(ctx, uuid)
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
	uuid, found := s.byName.Get(key)
	if !found {
		return nil, false
	}
	return s.cacheReadByUUID(ctx, uuid)
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
