package store

import (
	"context"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/meta"
	"github.com/relexec/rxp/types"
)

// Record decorates a Meta with internal DB information.
type Record struct {
	// RowID is the internal database SERIAL for the metas record.
	RowID int64
	// Meta is the publicly-exposed Meta object.
	Meta *meta.Meta
}

// ReadByRowID returns a Record for the Meta with the supplied internal DB
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

// ReadByKindVersion returns a Record for the Meta with the supplied
// KindVersion. This method will populate any caches with any read records.
func (s *Store) ReadByKindVersion(
	ctx context.Context,
	sys types.System,
	kv types.KindVersion,
) (*Record, error) {
	cacheKey := newByKindVersionCacheKey(sys, kv)
	cached, found := s.cacheReadByKindVersion(ctx, cacheKey)
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
	k := kv.Kind()
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
	record, err := s.dbReadByKindVersion(ctx, sysRec, kindRec, kv)
	if err != nil {
		return nil, err
	}
	err = s.cacheWrite(ctx, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}
