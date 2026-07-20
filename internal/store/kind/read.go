package store

import (
	"context"

	"github.com/relexec/rxp/api"

	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Record decorates a Kind with internal DB information.
type Record struct {
	// RowID is the internal database SERIAL for the kinds record.
	RowID int64
	// Kind is the publicly-exposed Kind object.
	Kind *api.Kind
}

// ReadByRowID returns a Record for the Kind with the supplied internal DB
// row ID. This method will populate any caches with any read records.
func (s *Store) ReadByRowID(
	ctx context.Context,
	rowID int64,
) (*Record, error) {
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

// ReadByUUID returns a Record for the Kind with the supplied UUID. This
// method will populate any caches with any read records.
func (s *Store) ReadByUUID(
	ctx context.Context,
	uuid string,
) (*Record, error) {
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

// ReadByName returns a Record for the Kind with the supplied Name. This
// method will populate any caches with any read records.
func (s *Store) ReadByName(
	ctx context.Context,
	sysRec storesystem.Record,
	name api.KindName,
) (*Record, error) {
	cacheKey := newByNameCacheKey(sysRec.System, name)
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
