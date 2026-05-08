package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/system"
	readoption "github.com/relexec/rxp/system/read/option"
	"github.com/relexec/rxp/system/read/selector"
	writeoption "github.com/relexec/rxp/system/write/option"
	rxptypes "github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type systemCacheKey string

// systemEntry decorates a System with internal DB information.
type systemEntry struct {
	// RowID is the internal database SERIAL for the systems record.
	RowID int64
	// System is the publicly-exposed System object.
	System *system.System
}

// SystemRead reads a System from persistent storage.
//
// If the system cache is enabled, this method automatically caches the
// returned System in the cache.
func (s *Store) SystemRead(
	ctx context.Context,
	sel selector.Selector,
	opts ...readoption.Option,
) (rxptypes.System, error) {
	err := s.requestValidate(ctx)
	if err != nil {
		return nil, err
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
		metrics.InstrumentReadRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentReadDuration.Record(ctx, elapsed)
	}()

	ropts := readoption.New(opts...)
	err = s.systemReadValidate(ctx, sel, ropts)
	if err != nil {
		return nil, err
	}

	uuid := sel.UUID()

	entry, err := s.systemRead(ctx, uuid)
	if err != nil {
		return nil, err
	}
	return entry.System, nil
}

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

// systemReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single System.
func (s *Store) systemReadValidate(
	ctx context.Context,
	sel selector.Selector,
	opts readoption.Options,
) error {
	return sel.Validate()
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

// systemRead returns a systemEntry for the supplied system UUID. This
// method will populate any caches with any read records.
func (s *Store) systemRead(
	ctx context.Context,
	uuid string,
) (*systemEntry, error) {
	cacheKey := systemCacheKey(uuid)
	cached, found := s.systemCacheRead(ctx, cacheKey)
	if found {
		return cached, nil
	}
	entry, err := s.systemDBRead(ctx, uuid)
	if err != nil {
		return nil, err
	}
	err = s.systemCacheWrite(ctx, cacheKey, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

// systemCacheRead returns a cached System and whether or not the supplied
// system name was found in the cache.
func (s *Store) systemCacheRead(
	ctx context.Context,
	key systemCacheKey,
) (*systemEntry, bool) {
	if s.systemCache == nil {
		return nil, false
	}
	return s.systemCache.Get(key)
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

// systemDBRead performs a SELECT query to return the stored system record.
func (s *Store) systemDBRead(
	ctx context.Context,
	uuid string,
) (*systemEntry, error) {
	out := systemEntry{
		System: system.New(
			system.WithUUID(uuid),
		),
	}
	fn := func(tx pgx.Tx) error {
		qs := "SELECT id FROM systems WHERE uuid = $1"
		err := tx.QueryRow(ctx, qs, uuid).Scan(&out.RowID)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading systems record",
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

// systemDBWrite inserts the supplied system information into the database.
func (s *Store) systemDBWrite(
	ctx context.Context,
	system rxptypes.System,
) error {
	uuid := system.UUID()
	fn := func(tx pgx.Tx) error {
		qs := "INSERT INTO systems (uuid) VALUES ($1)"
		_, err := tx.Exec(ctx, qs, uuid)
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
