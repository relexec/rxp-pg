package store

import (
	"context"
	"fmt"

	"github.com/relexec/rxp/errors"
)

type byRowIDCacheKey int64
type byUUIDCacheKey string

// cacheReadByRowID looks up a cached System by RowID, returning the cached
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

// cacheReadByUUID looks up a cached System by UUID, returning the cached
// Record and whether or not the entry  was found.
func (s *Store) cacheReadByUUID(
	ctx context.Context,
	key byUUIDCacheKey,
) (*Record, bool) {
	if s.byUUID == nil {
		return nil, false
	}
	return s.byUUID.Get(key)
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
	key := byUUIDCacheKey(rec.System.UUID())
	set := s.byUUID.Set(key, rec)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting system cache key %q", key),
		)
	}
	return nil
}
