package store

import (
	"context"

	apidomain "github.com/relexec/rxp/api/domain"
	apisystem "github.com/relexec/rxp/api/system"
)

// ReadByRowID returns a apidomain.Domain for the Domain with the supplied internal
// DB row ID. This method will populate any caches with any read records.
func (s *Store) ReadByRowID(
	ctx context.Context,
	sysRec *apisystem.System,
	rowID int64,
) (*apidomain.Domain, error) {
	cacheKey := byRowIDCacheKey(rowID)
	cached, found := s.cacheReadByRowID(ctx, cacheKey)
	if found {
		return cached, nil
	}
	record, err := s.dbReadByRowID(ctx, sysRec, rowID)
	if err != nil {
		return nil, err
	}
	err = s.cacheWrite(ctx, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

// ReadByUUID returns a apidomain.Domain for the Domain with the supplied UUID. This
// method will populate any caches with any read records.
func (s *Store) ReadByUUID(
	ctx context.Context,
	sysRec *apisystem.System,
	uuid string,
) (*apidomain.Domain, error) {
	cacheKey := byUUIDCacheKey(uuid)
	cached, found := s.cacheReadByUUID(ctx, cacheKey)
	if found {
		return cached, nil
	}
	record, err := s.dbReadByUUID(ctx, sysRec, uuid)
	if err != nil {
		return nil, err
	}
	err = s.cacheWrite(ctx, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

// ReadByName returns a apidomain.Domain for the Domain with the supplied Name. This
// method will populate any caches with any read records.
func (s *Store) ReadByName(
	ctx context.Context,
	sysRec *apisystem.System,
	name apidomain.Name,
) (*apidomain.Domain, error) {
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
