package store

import (
	"context"

	"github.com/relexec/rxp/api"
	apisystem "github.com/relexec/rxp/api/system"
)

// ReadByRowID returns a api.Kind for the Kind with the supplied internal DB
// row ID. This method will populate any caches with any read records.
func (s *Store) ReadByRowID(
	ctx context.Context,
	rowID int64,
) (*api.Kind, error) {
	cacheKey := byRowIDCacheKey(rowID)
	cached, found := s.cacheReadByRowID(ctx, cacheKey)
	if found {
		return cached, nil
	}
	record, err := s.dbReadByRowID(ctx, rowID)
	if err != nil {
		return nil, err
	}
	err = s.cacheWrite(ctx, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

// ReadByUUID returns a api.Kind for the Kind with the supplied UUID. This
// method will populate any caches with any read records.
func (s *Store) ReadByUUID(
	ctx context.Context,
	uuid string,
) (*api.Kind, error) {
	cacheKey := byUUIDCacheKey(uuid)
	cached, found := s.cacheReadByUUID(ctx, cacheKey)
	if found {
		return cached, nil
	}
	record, err := s.dbReadByUUID(ctx, uuid)
	if err != nil {
		return nil, err
	}
	err = s.cacheWrite(ctx, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

// ReadByName returns a api.Kind for the Kind with the supplied Name. This
// method will populate any caches with any read records.
func (s *Store) ReadByName(
	ctx context.Context,
	sysRec *apisystem.System,
	name api.KindName,
) (*api.Kind, error) {
	cacheKey := newByNameCacheKey(sysRec, name)
	cached, found := s.cacheReadByName(ctx, cacheKey)
	if found {
		return cached, nil
	}
	record, err := s.dbReadByName(ctx, sysRec, name)
	if err != nil {
		return nil, err
	}
	err = s.cacheWrite(ctx, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}
