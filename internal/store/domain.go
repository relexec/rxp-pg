package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/domain"
	readoption "github.com/relexec/rxp/domain/read/option"
	"github.com/relexec/rxp/domain/read/selector"
	writeoption "github.com/relexec/rxp/domain/write/option"
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
	// Default the system to the host system if it hasn't been specified.
	if system == nil {
		system = s.hostSystem.System
	}
	systemEntry, err := s.systemRead(ctx, system.UUID())
	if err != nil {
		return nil, err
	}

	name := sel.Name()

	entry, err := s.domainRead(ctx, systemEntry, name)
	if err != nil {
		return nil, err
	}
	return entry.Domain, nil
}

// DomainWrite atomically writes the supplied Domain to persistent storage.
func (s *Store) DomainWrite(
	ctx context.Context,
	domain rxptypes.Domain,
	opts ...writeoption.Option,
) error {
	err := s.requestValidate(ctx)
	if err != nil {
		return err
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
		metrics.InstrumentWriteRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentWriteDuration.Record(ctx, elapsed)
	}()

	wopts := writeoption.New(opts...)
	err = s.domainWriteValidate(ctx, domain, wopts)
	if err != nil {
		return err
	}

	system := domain.System()
	// Default the system to the host system if it hasn't been specified.
	if system == nil {
		system = s.hostSystem.System
	}
	systemEntry, err := s.systemRead(ctx, system.UUID())
	if err != nil {
		return err
	}
	return s.domainDBWrite(ctx, systemEntry, domain)
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

// domainWriteValidate returns an error if the supplied domain and write
// options are not valid for writing a single Domain.
func (s *Store) domainWriteValidate(
	ctx context.Context,
	domain rxptypes.Domain,
	opts writeoption.Options,
) error {
	return domain.Validate()
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

// domainCacheWrite writes the supplied cache entry if the domain cache is
// enabled.
func (s *Store) domainCacheWrite(
	ctx context.Context,
	key domainCacheKey,
	entry *domainEntry,
) error {
	if s.domainCache == nil {
		return nil
	}
	set := s.domainCache.Set(key, entry)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting domain cache key %q", key),
		)
	}
	return nil
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
		qs := "SELECT id FROM domains WHERE system = $1 AND name = $2"
		err := tx.QueryRow(ctx, qs, system.RowID, name).Scan(&out.RowID)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domains record",
				errors.WithWrap(err),
			)
		}
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// domainDBWrite inserts the supplied domain information into the database.
func (s *Store) domainDBWrite(
	ctx context.Context,
	systemEntry *systemEntry,
	domain rxptypes.Domain,
) error {
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	fn := func(tx pgx.Tx) error {
		qs := "INSERT INTO domains (system, name, last_modified_on, last_modified_by) VALUES ($1, $2, $3, $4)"
		_, err := tx.Exec(ctx, qs, systemEntry.RowID, domain.Name(), createdOn, createdBy)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.DuplicateName("domain", domain.Name())
				}
			}
		}
		return err
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return errors.Internal(
			"failed inserting domains record",
			errors.WithWrap(err),
		)
	}
	return nil
}
