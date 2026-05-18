package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/types"
)

type byUUIDCacheKey string
type byNameCacheKey string

func (k byNameCacheKey) DomainUUID() string {
	parts := strings.Split(string(k), "|")
	return parts[0]
}

func (k byNameCacheKey) NamespaceName() types.NamespaceName {
	parts := strings.Split(string(k), "|")
	return types.NamespaceName(parts[1])
}

func newByNameCacheKey(
	domain types.Domain,
	name types.NamespaceName,
) byNameCacheKey {
	return byNameCacheKey(domain.UUID() + "|" + string(name))
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

// cacheReadByName looks up a cached Domain by Domain UUID + Name, returning the cached
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
	if s.byUUID == nil {
		return nil
	}
	uuidKey := byUUIDCacheKey(rec.Namespace.UUID())
	set := s.byUUID.Set(uuidKey, rec)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting domain cache uuid key %q", uuidKey),
		)
	}
	nameKey := newByNameCacheKey(rec.Namespace.Domain(), rec.Namespace.Name())
	set = s.byName.Set(nameKey, rec)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting domain cache name key %q", nameKey),
		)
	}
	return nil
}
