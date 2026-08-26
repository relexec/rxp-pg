package store

import (
	"context"

	apikind "github.com/relexec/rxp/api/kind"
	apikindversion "github.com/relexec/rxp/api/kindversion"
	apisystem "github.com/relexec/rxp/api/system"
)

// ReadByRowID returns a apikindversion.KindVersion for the KindVersion with the supplied internal DB
// row ID. This method will populate any caches with any read records.
func (s *Store) ReadByRowID(
	ctx context.Context,
	sysRec *apisystem.System,
	kindRec *apikind.Kind,
	rowID int64,
) (*apikindversion.KindVersion, error) {
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

// ReadByName returns a apikindversion.KindVersion for the KindVersion with the supplied
// Name. This method will populate any caches with any read records.
func (s *Store) ReadByName(
	ctx context.Context,
	sysRec *apisystem.System,
	kindRec *apikind.Kind,
	name apikindversion.Name,
) (*apikindversion.KindVersion, error) {
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
