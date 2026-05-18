package store

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/domain/read"
	readoption "github.com/relexec/rxp/domain/read/option"
	"github.com/relexec/rxp/domain/read/selector"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	rxptypes "github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type domainCacheKey string

func (k domainCacheKey) SystemUUID() string {
	parts := strings.Split(string(k), "|")
	return parts[0]
}

func (k domainCacheKey) DomainName() rxptypes.DomainName {
	parts := strings.Split(string(k), "|")
	return rxptypes.DomainName(parts[1])
}

// domainEntry decorates a Domain with internal DB information.
type domainEntry struct {
	// RowID is the internal database SERIAL for the domains record.
	RowID int64
	// SystemRowID is the internal database SERIAL for the domain's associated
	// system record.
	SystemRowID int64
	// Domain is the publicly-exposed Domain object.
	Domain *domain.Domain
}

func newDomainCacheKey(
	system rxptypes.System,
	name rxptypes.DomainName,
) domainCacheKey {
	return domainCacheKey(system.UUID() + "|" + string(name))
}

// DomainRead reads a Domain from persistent storage.
//
// If the domain cache is enabled, this method automatically caches the
// returned Domain in the cache.
func (s *Store) DomainRead(
	ctx context.Context,
	sel selector.Selector,
	opts ...readoption.Option,
) (rxptypes.Domain, error) {
	err := s.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeTargetType(metrics.TargetTypeDomain),
		}
		if err != nil {
			attrs = append(attrs, metrics.AttributeErrCode(err))
		}
		metrics.InstrumentReadRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentReadDuration.Record(ctx, elapsed)
	}()

	ropts := readoption.New(opts...)
	err = s.domainReadValidate(ctx, sel, ropts)
	if err != nil {
		return nil, err
	}

	system := sel.System()
	var systemEntry *systemEntry
	// Default the system to the host system if it hasn't been specified.
	if system == nil {
		systemEntry = s.hostSystem
	} else {
		systemEntry, err = s.systemRead(ctx, system.UUID())
		if err != nil {
			return nil, err
		}
	}

	name := sel.Name()

	entry, err := s.domainRead(ctx, systemEntry, name)
	if err != nil {
		return nil, err
	}
	return entry.Domain, nil
}

// domainReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Domain.
func (s *Store) domainReadValidate(
	ctx context.Context,
	sel selector.Selector,
	opts readoption.Options,
) error {
	return sel.Validate()
}

// domainRead returns a domainEntry for the supplied pre-validated system entry
// and domain name. This method will populate any caches with any read records.
func (s *Store) domainRead(
	ctx context.Context,
	systemEntry *systemEntry,
	name rxptypes.DomainName,
) (*domainEntry, error) {
	system := systemEntry.System
	cacheKey := newDomainCacheKey(system, name)
	cached, found := s.domainCacheRead(ctx, cacheKey)
	if found {
		return cached, nil
	}

	entry, err := s.domainDBRead(ctx, systemEntry, name)
	if err != nil {
		return nil, err
	}
	err = s.domainCacheWrite(ctx, cacheKey, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

// domainCacheRead returns a cached Domain and whether or not the supplied
// domain name was found in the cache.
func (s *Store) domainCacheRead(
	ctx context.Context,
	key domainCacheKey,
) (*domainEntry, bool) {
	if s.domainCache == nil {
		return nil, false
	}
	return s.domainCache.Get(key)
}

// domainDBRead performs a SELECT query to return the stored domain record.
func (s *Store) domainDBRead(
	ctx context.Context,
	system *systemEntry,
	name rxptypes.DomainName,
) (*domainEntry, error) {
	out := domainEntry{
		Domain: domain.New(
			domain.WithSystem(system.System),
			domain.WithName(name),
		),
		SystemRowID: system.RowID,
	}
	fn := func(tx pgx.Tx) error {
		var uuid string
		qs := "SELECT id, uuid FROM domains WHERE system = $1 AND name = $2"
		err := tx.QueryRow(
			ctx, qs, system.RowID, name,
		).Scan(&out.RowID, &uuid)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domains record",
				errors.WithWrap(err),
			)
		}
		out.Domain.SetUUID(uuid)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

var _ read.DomainReader = (*Store)(nil)
