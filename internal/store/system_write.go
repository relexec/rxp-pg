package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/system/write"
	writeoption "github.com/relexec/rxp/system/write/option"
	rxptypes "github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SystemWrite atomically writes the supplied System to persistent storage.
func (s *Store) SystemWrite(
	ctx context.Context,
	system rxptypes.System,
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
			metrics.AttributeTargetType(metrics.TargetTypeSystem),
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
	err = s.systemWriteValidate(ctx, system, wopts)
	if err != nil {
		return err
	}
	return s.systemDBWrite(ctx, system)
}

// systemWriteValidate returns an error if the supplied system and write
// options are not valid for writing a single System.
func (s *Store) systemWriteValidate(
	ctx context.Context,
	system rxptypes.System,
	opts writeoption.Options,
) error {
	return system.Validate()
}

// systemCacheWrite writes the supplied cache entry if the system cache is
// enabled.
func (s *Store) systemCacheWrite(
	ctx context.Context,
	key systemCacheKey,
	entry *systemEntry,
) error {
	if s.systemCache == nil {
		return nil
	}
	set := s.systemCache.Set(key, entry)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting system cache key %q", key),
		)
	}
	return nil
}

// systemDBWrite inserts the supplied system information into the database.
func (s *Store) systemDBWrite(
	ctx context.Context,
	system rxptypes.System,
) error {
	var name *string
	uuid := system.UUID()
	sysName := system.Name()
	if sysName != "" {
		name = &sysName
	}
	fn := func(tx pgx.Tx) error {
		qs := "INSERT INTO systems (uuid, name) VALUES ($1, $2)"
		_, err := tx.Exec(ctx, qs, uuid, name)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.DuplicateKey("system", "uuid", system.UUID())
				}
			}
		}
		return err
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return errors.Internal(
			"failed inserting systems record",
			errors.WithWrap(err),
		)
	}
	return nil
}

var _ write.SystemWriter = (*Store)(nil)
