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
type byKindVersionCacheKey string

func (k byKindVersionCacheKey) SystemUUID() string {
	parts := strings.Split(string(k), "|")
	return parts[0]
}

func (k byKindVersionCacheKey) KindVersion() api.KindVersion {
	parts := strings.Split(string(k), "|")
	return api.KindVersion(parts[1])
}

func newByKindVersionCacheKey(
	system *system.System,
	kv api.KindVersion,
) byKindVersionCacheKey {
	return byKindVersionCacheKey(system.UUID() + "|" + string(kv))
}

// cacheReadByRowID looks up a cached Meta by RowID, returning the cached
// Record and whether or not the entry was found.
func (s *Store) cacheReadByRowID(
	ctx context.Context,
	key byRowIDCacheKey,
) (*Record, bool) {
	if s.byRowID == nil {
		return nil, false
	}
	kv, found := s.byRowID.Get(key)
	if !found {
		return nil, false
	}
	return s.cacheReadByKindVersion(ctx, kv)
}

// cacheReadByKindVersion looks up a cached Meta by KindVersion, returning the cached
// Record and whether or not the entry was found.
func (s *Store) cacheReadByKindVersion(
	ctx context.Context,
	key byKindVersionCacheKey,
) (*Record, bool) {
	if s.byKindVersion == nil {
		return nil, false
	}
	return s.byKindVersion.Get(key)
}

// cacheWrite ensures the supplied Record is written to the lookup caches if
// enabled.
func (s *Store) cacheWrite(
	ctx context.Context,
	rec *Record,
) error {
	if s.byKindVersion == nil {
		return nil
	}
	kvKey := newByKindVersionCacheKey(
		rec.Meta.System(),
		rec.Meta.KindVersion(),
	)
	set := s.byKindVersion.Set(kvKey, rec)
	if !set {
		return errors.Internal(
			fmt.Sprintf(
				"failed setting meta cache kindversion key %q", kvKey,
			),
		)
	}
	// Here we populate our row ID -> kv map
	rowIDKey := byRowIDCacheKey(rec.RowID)
	set = s.byRowID.Set(rowIDKey, kvKey)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting meta cache rowid key %q", rowIDKey),
		)
	}
	return nil
}
