package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/system"
	"github.com/relexec/rxp/system/read"
	readoption "github.com/relexec/rxp/system/read/option"
	"github.com/relexec/rxp/system/read/selector"
	rxptypes "github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type systemCacheKey string // System.UUID is the cache key

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

// systemReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single System.
func (s *Store) systemReadValidate(
	ctx context.Context,
	sel selector.Selector,
	opts readoption.Options,
) error {
	return sel.Validate()
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
		var name sql.NullString
		qs := "SELECT id, name FROM systems WHERE uuid = $1"
		err := tx.QueryRow(ctx, qs, uuid).Scan(&out.RowID, &name)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading systems record",
				errors.WithWrap(err),
			)
		}
		if name.Valid {
			out.System.SetName(name.String)
		}
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

var _ read.SystemReader = (*Store)(nil)
