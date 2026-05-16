package store

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind"
	"github.com/relexec/rxp/kind/read"
	readoption "github.com/relexec/rxp/kind/read/option"
	"github.com/relexec/rxp/kind/read/selector"
	"github.com/relexec/rxp/metrics"
	rxptypes "github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type kindCacheKey string

func (k kindCacheKey) SystemUUID() string {
	parts := strings.Split(string(k), "|")
	return parts[0]
}

func (k kindCacheKey) KindName() rxptypes.KindName {
	parts := strings.Split(string(k), "|")
	return rxptypes.KindName(parts[1])
}

// kindEntry decorates a Kind with internal DB information.
type kindEntry struct {
	// RowID is the internal database SERIAL for the kinds record.
	RowID int64
	// SystemRowID is the internal database SERIAL for the kind's associated
	// system record.
	SystemRowID int64
	// Kind is the publicly-exposed Kind object.
	Kind *kind.Kind
}

func newKindCacheKey(
	system rxptypes.System,
	name rxptypes.KindName,
) kindCacheKey {
	return kindCacheKey(system.UUID() + "|" + string(name))
}

// KindRead reads a Kind from persistent storage.
//
// If the kind cache is enabled, this method automatically caches the
// returned Kind in the cache.
func (s *Store) KindRead(
	ctx context.Context,
	sel selector.Selector,
	opts ...readoption.Option,
) (rxptypes.Kind, error) {
	err := s.requestValidate(ctx)
	if err != nil {
		return nil, err
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
		metrics.InstrumentReadRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentReadDuration.Record(ctx, elapsed)
	}()

	ropts := readoption.New(opts...)
	err = s.kindReadValidate(ctx, sel, ropts)
	if err != nil {
		return nil, err
	}

	system := sel.System()
	if system == nil {
		system = s.hostSystem.System
	}
	systemEntry, err := s.systemRead(ctx, system.UUID())
	if err != nil {
		return nil, err
	}

	name := sel.Name()
	entry, err := s.kindRead(ctx, systemEntry, name)
	if err != nil {
		return nil, err
	}
	return entry.Kind, nil
}

// kindReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Kind.
func (s *Store) kindReadValidate(
	ctx context.Context,
	sel selector.Selector,
	opts readoption.Options,
) error {
	return sel.Validate()
}

// kindRead returns a kindEntry for the supplied pre-validated system
// and kind name. This method will populate any caches with any read records.
func (s *Store) kindRead(
	ctx context.Context,
	systemEntry *systemEntry,
	name rxptypes.KindName,
) (*kindEntry, error) {
	system := systemEntry.System
	cacheKey := newKindCacheKey(system, name)
	cached, found := s.kindCacheRead(ctx, cacheKey)
	if found {
		return cached, nil
	}

	systemEntry, err := s.systemRead(ctx, system.UUID())
	if err != nil {
		return nil, err
	}

	entry, err := s.kindDBRead(ctx, systemEntry, name)
	if err != nil {
		return nil, err
	}
	err = s.kindCacheWrite(ctx, cacheKey, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

// kindCacheRead returns a cached Kind and whether or not the supplied
// kind name was found in the cache.
func (s *Store) kindCacheRead(
	ctx context.Context,
	key kindCacheKey,
) (*kindEntry, bool) {
	if s.kindCache == nil {
		return nil, false
	}
	return s.kindCache.Get(key)
}

// kindDBRead performs a SELECT query to return the stored kind record.
func (s *Store) kindDBRead(
	ctx context.Context,
	systemEntry *systemEntry,
	name rxptypes.KindName,
) (*kindEntry, error) {
	out := kindEntry{
		Kind: kind.New(
			kind.WithSystem(systemEntry.System),
			kind.WithName(name),
		),
		SystemRowID: systemEntry.RowID,
	}
	var namescope rxptypes.Namescope
	fn := func(tx pgx.Tx) error {
		qs := "SELECT id, namescope FROM kinds WHERE system = $1 AND name = $2"
		err := tx.QueryRow(
			ctx, qs, systemEntry.RowID, name,
		).Scan(
			&out.RowID, &namescope,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading kinds record",
				errors.WithWrap(err),
			)
		}
		out.Kind.SetNamescope(namescope)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

var _ read.KindReader = (*Store)(nil)
