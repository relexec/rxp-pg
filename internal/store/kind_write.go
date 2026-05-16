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
	"github.com/relexec/rxp/kind/write"
	writeoption "github.com/relexec/rxp/kind/write/option"
	"github.com/relexec/rxp/metrics"
	rxptypes "github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// KindWrite atomically writes the supplied Kind to persistent storage.
func (s *Store) KindWrite(
	ctx context.Context,
	kind rxptypes.Kind,
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
			metrics.AttributeTargetType(metrics.TargetTypeKind),
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
	err = s.kindWriteValidate(ctx, kind, wopts)
	if err != nil {
		return err
	}

	system := kind.System()
	if system == nil {
		system = s.hostSystem.System
	}

	systemEntry, err := s.systemRead(ctx, system.UUID())
	if err != nil {
		return err
	}
	return s.kindDBWrite(ctx, systemEntry, kind)
}

// kindWriteValidate returns an error if the supplied kind and write
// options are not valid for writing a single Kind.
func (s *Store) kindWriteValidate(
	ctx context.Context,
	kind rxptypes.Kind,
	opts writeoption.Options,
) error {
	return kind.Validate()
}

// kindCacheWrite writes the supplied cache entry if the kind cache is
// enabled.
func (s *Store) kindCacheWrite(
	ctx context.Context,
	key kindCacheKey,
	entry *kindEntry,
) error {
	if s.kindCache == nil {
		return nil
	}
	set := s.kindCache.Set(key, entry)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting kind cache key %q", key),
		)
	}
	return nil
}

// kindDBWrite inserts the supplied kind information into the database.
func (s *Store) kindDBWrite(
	ctx context.Context,
	systemEntry *systemEntry,
	kind rxptypes.Kind,
) error {
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	fn := func(tx pgx.Tx) error {
		qs := `
INSERT INTO kinds (
  system
, name
, namescope
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
			ctx, qs, systemEntry.RowID,
			kind.Name(), kind.Namescope(),
			createdOn, createdBy,
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.DuplicateName("kind", kind.Name())
				}
			}
		}
		return err
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return errors.Internal(
			"failed inserting kinds record",
			errors.WithWrap(err),
		)
	}
	return nil
}

var _ write.KindWriter = (*Store)(nil)
