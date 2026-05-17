package store

import (
	"context"
	"fmt"

	"github.com/relexec/rxp/errors"
)

type byUUIDCacheKey string // System.UUID is the cache key

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
