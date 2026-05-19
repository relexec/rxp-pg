package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/types"
)

type byRowIDCacheKey int64
type byUUIDCacheKey string
type byNameCacheKey string

func (k byNameCacheKey) SystemUUID() string {
	parts := strings.Split(string(k), "|")
	return parts[0]
}

func (k byNameCacheKey) KindName() types.KindName {
	parts := strings.Split(string(k), "|")
	return types.KindName(parts[1])
}

func newByNameCacheKey(
	system types.System,
	name types.KindName,
) byNameCacheKey {
	return byNameCacheKey(system.UUID() + "|" + string(name))
}

// cacheReadByRowID looks up a cached Kind by RowID, returning the cached
// Record and whether or not the entry was found.
func (s *Store) cacheReadByRowID(
	ctx context.Context,
	key byRowIDCacheKey,
) (*Record, bool) {
	if s.byRowID == nil {
		return nil, false
	}
	name, found := s.byRowID.Get(key)
	if !found {
		return nil, false
	}
	return s.cacheReadByName(ctx, name)
}

// cacheReadByName looks up a cached Kind by System UUID + Name, returning the cached
// Record and whether or not the entry was found.
func (s *Store) cacheReadByName(
	ctx context.Context,
	key byNameCacheKey,
) (*Record, bool) {
	if s.byName == nil {
		return nil, false
	}
	return s.byName.Get(key)
}

// cacheWrite ensures the supplied Record is written to the lookup caches if
// enabled.
func (s *Store) cacheWrite(
	ctx context.Context,
	rec *Record,
) error {
	if s.byName == nil {
		return nil
	}
	nameKey := newByNameCacheKey(rec.Kind.System(), rec.Kind.Name())
	set := s.byName.Set(nameKey, rec)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting kind cache name key %q", nameKey),
		)
	}
	// Here we populate our row ID -> name map.
	rowIDKey := byRowIDCacheKey(rec.RowID)
	set = s.byRowID.Set(rowIDKey, nameKey)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting kind cache rowid key %q", rowIDKey),
		)
	}
	return nil
}
