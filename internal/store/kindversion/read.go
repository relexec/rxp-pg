package store

import (
	"context"

	"github.com/relexec/rxp/api"
	apikind "github.com/relexec/rxp/api/kind"
	apisystem "github.com/relexec/rxp/api/system"
)

// ReadByRowID returns a api.KindVersion for the KindVersion with the supplied internal DB
// row ID. This method will populate any caches with any read records.
func (s *Store) ReadByRowID(
	ctx context.Context,
	sysRec *apisystem.System,
	kindRec *apikind.Kind,
	rowID int64,
) (*api.KindVersion, error) {
	cacheKey := byRowIDCacheKey(rowID)
	cached, found := s.cacheReadByRowID(ctx, cacheKey)
	if found {
		return cached, nil
	}
	record, err := s.dbReadByRowID(ctx, sysRec, kindRec, rowID)
	if err != nil {
		return nil, err
	}
	err = s.cacheWrite(ctx, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

// ReadByName returns a api.KindVersion for the KindVersion with the supplied
// KindVersionName. This method will populate any caches with any read records.
func (s *Store) ReadByName(
	ctx context.Context,
	sysRec *apisystem.System,
	kindRec *apikind.Kind,
	name api.KindVersionName,
) (*api.KindVersion, error) {
	cacheKey := newByNameCacheKey(sysRec, name)
	cached, found := s.cacheReadByName(ctx, cacheKey)
	if found {
		return cached, nil
	}
	record, err := s.dbReadByName(ctx, sysRec, kindRec, name)
	if err != nil {
		return nil, err
	}
	err = s.cacheWrite(ctx, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}
