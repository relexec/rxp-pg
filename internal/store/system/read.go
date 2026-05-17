package store

import (
	"context"

	"github.com/relexec/rxp/system"
)

// Record decorates a System with internal DB information.
type Record struct {
	// RowID is the internal database SERIAL for the systems record.
	RowID int64
	// System is the publicly-exposed System object.
	System *system.System
}

// ReadByUUID returns a Record for the System with the supplied UUID. This
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
