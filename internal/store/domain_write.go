package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/domain/write"
	writeoption "github.com/relexec/rxp/domain/write/option"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	rxptypes "github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

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

// domainWriteValidate returns an error if the supplied domain and write
// options are not valid for writing a single Domain.
func (s *Store) domainWriteValidate(
	ctx context.Context,
	domain rxptypes.Domain,
	opts writeoption.Options,
) error {
	return domain.Validate()
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

// domainDBWrite inserts the supplied domain information into the database.
func (s *Store) domainDBWrite(
	ctx context.Context,
	systemEntry *systemEntry,
	domain rxptypes.Domain,
) error {
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	fn := func(tx pgx.Tx) error {
		qs := `
INSERT INTO domains (
  system
, uuid
, name
, last_modified_on
, last_modified_by
) VALUES (
  $1
, $2
, $3
, $4
, $5
)`
		_, err := tx.Exec(
			ctx, qs,
			systemEntry.RowID,
			domain.UUID(),
			domain.Name(),
			createdOn,
			createdBy,
		)
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

var _ write.DomainWriter = (*Store)(nil)
