package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	writeoption "github.com/relexec/rxp/namespace/write/option"
	rxptypes "github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// NamespaceWrite atomically writes the supplied Namespace to persistent storage.
func (s *Store) NamespaceWrite(
	ctx context.Context,
	namespace rxptypes.Namespace,
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
			metrics.AttributeTargetType(metrics.TargetTypeNamespace),
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
	err = s.namespaceWriteValidate(ctx, namespace, wopts)
	if err != nil {
		return err
	}

	domain := namespace.Domain()
	system := domain.System()
	// Default the system to the host system if it hasn't been specified.
	if system == nil {
		system = s.hostSystem.System
	}
	systemEntry, err := s.systemRead(ctx, system.UUID())
	if err != nil {
		return err
	}

	domainEntry, err := s.domainRead(ctx, systemEntry, domain.Name())
	if err != nil {
		return err
	}

	return s.namespaceDBWrite(ctx, domainEntry, namespace)
}

// namespaceWriteValidate returns an error if the supplied namespace and write
// options are not valid for writing a single Namespace.
func (s *Store) namespaceWriteValidate(
	ctx context.Context,
	namespace rxptypes.Namespace,
	opts writeoption.Options,
) error {
	return namespace.Validate()
}

// namespaceCacheWrite writes the supplied cache entry if the namespace cache is
// enabled.
func (s *Store) namespaceCacheWrite(
	ctx context.Context,
	key namespaceCacheKey,
	entry *namespaceEntry,
) error {
	if s.namespaceCache == nil {
		return nil
	}
	set := s.namespaceCache.Set(key, entry)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting namespace cache key %q", key),
		)
	}
	return nil
}

// namespaceDBWrite inserts the supplied namespace information into the database.
func (s *Store) namespaceDBWrite(
	ctx context.Context,
	domainEntry *domainEntry,
	namespace rxptypes.Namespace,
) error {
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	fn := func(tx pgx.Tx) error {
		qs := "INSERT INTO namespaces (domain, name, last_modified_on, last_modified_by) VALUES ($1, $2, $3, $4)"
		_, err := tx.Exec(ctx, qs, domainEntry.RowID, namespace.Name(), createdOn, createdBy)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.DuplicateName("namespace", namespace.Name())
				}
			}
		}
		return err
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return errors.Internal(
			"failed inserting namespaces record",
			errors.WithWrap(err),
		)
	}
	return nil
}
