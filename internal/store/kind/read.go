package store

import (
	"context"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind"
	"github.com/relexec/rxp/types"
)

// Record decorates a Kind with internal DB information.
type Record struct {
	// RowID is the internal database SERIAL for the kinds record.
	RowID int64
	// Kind is the publicly-exposed Kind object.
	Kind *kind.Kind
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

// ReadByName returns a Record for the Kind with the supplied Name. This
// method will populate any caches with any read records.
func (s *Store) ReadByName(
	ctx context.Context,
	sys types.System,
	name types.KindName,
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
