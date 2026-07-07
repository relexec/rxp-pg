package store

import (
	"context"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/domain"

	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Record decorates a Domain with internal DB information.
type Record struct {
	// RowID is the internal database SERIAL for the domains record.
	RowID int64
	// Root is the internal database SERIAL for the domains record that is the
	// root of this "domain tree".
	Root int64
	// Left is the nested set model's left side value for this node.
	Left int64
	// Right is the nested set model's right side value for this node.
	Right int64
	// Domain is the publicly-exposed Domain object.
	Domain *domain.Domain
}

// ReadByRowID returns a Record for the Domain with the supplied internal DB
// row ID. This method will populate any caches with any read records.
func (s *Store) ReadByRowID(
	ctx context.Context,
	sysRec storesystem.Record,
	rowID int64,
) (*Record, error) {
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

// ReadByUUID returns a Record for the Domain with the supplied UUID. This
// method will populate any caches with any read records.
func (s *Store) ReadByUUID(
	ctx context.Context,
	sysRec storesystem.Record,
	uuid string,
) (*Record, error) {
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

// ReadByName returns a Record for the Domain with the supplied Name. This
// method will populate any caches with any read records.
func (s *Store) ReadByName(
	ctx context.Context,
	sysRec storesystem.Record,
	name api.DomainName,
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
