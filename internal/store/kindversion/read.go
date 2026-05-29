package store

import (
	"context"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind/kindversion"
	"github.com/relexec/rxp/system"
)

// Record decorates a KindVersion with internal DB information.
type Record struct {
	// RowID is the internal database SERIAL for the kindversions record.
	RowID int64
	// KindVersion is the publicly-exposed KindVersion object.
	KindVersion *kindversion.KindVersion
}

// ReadByRowID returns a Record for the KindVersion with the supplied internal DB
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

// ReadByName returns a Record for the KindVersion with the supplied
// KindVersionName. This method will populate any caches with any read records.
func (s *Store) ReadByName(
	ctx context.Context,
	sys *system.System,
	name api.KindVersionName,
) (*Record, error) {
	cacheKey := newByNameCacheKey(sys, name)
	cached, found := s.cacheReadByName(ctx, cacheKey)
	if found {
		return cached, nil
	}
	sysRec, err := s.systemStore.ReadByUUID(ctx, sys.UUID())
	if err != nil {
		return nil, errors.Internal(
			"failed reading system record",
			errors.WithWrap(err),
		)
	}
	k := name.Kind()
	kindRec, err := s.kindStore.ReadByName(ctx, sys, k)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrKindVersionUnknown
		}
		return nil, errors.Internal(
			"failed reading kind record",
			errors.WithWrap(err),
		)
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
