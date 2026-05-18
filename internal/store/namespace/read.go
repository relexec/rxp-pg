package store

import (
	"context"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/namespace"
	"github.com/relexec/rxp/types"

	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
)

// Record decorates a Namespace with internal DB information.
type Record struct {
	// RowID is the internal database SERIAL for the namespaces record.
	RowID int64
	// DomainRecord contains information about Domain containing this
	// Namespace.
	DomainRecord *storedomain.Record
	// Namespace is the publicly-exposed Namespace object.
	Namespace *namespace.Namespace
}

// ReadByUUID returns a Record for the Namespace with the supplied UUID. This
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

// ReadByName returns a Record for the Namespace with the supplied Name. This
// method will populate any caches with any read records.
func (s *Store) ReadByName(
	ctx context.Context,
	dom types.Domain,
	name types.NamespaceName,
) (*Record, error) {
	cacheKey := newByNameCacheKey(dom, name)
	cached, found := s.cacheReadByName(ctx, cacheKey)
	if found {
		return cached, nil
	}
	domRec, err := s.domainStore.ReadByUUID(ctx, dom.UUID())
	if err != nil {
		return nil, errors.Internal(
			"failed reading domain record",
			errors.WithWrap(err),
		)
	}
	record, err := s.dbReadByName(ctx, domRec, name)
	if err != nil {
		return nil, err
	}
	err = s.cacheWrite(ctx, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}
