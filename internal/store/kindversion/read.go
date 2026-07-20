package store

import (
	"context"

	"github.com/relexec/rxp/api"

	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Record decorates a KindVersion with internal DB information.
type Record struct {
	// RowID is the internal database SERIAL for the kindversions record.
	RowID int64
	// KindVersion is the publicly-exposed KindVersion object.
	KindVersion *api.KindVersion
}

// ReadByRowID returns a Record for the KindVersion with the supplied internal DB
// row ID. This method will populate any caches with any read records.
func (s *Store) ReadByRowID(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	rowID int64,
) (*Record, error) {
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

// ReadByName returns a Record for the KindVersion with the supplied
// KindVersionName. This method will populate any caches with any read records.
func (s *Store) ReadByName(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	name api.KindVersionName,
) (*Record, error) {
	cacheKey := newByNameCacheKey(sysRec.System, name)
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
