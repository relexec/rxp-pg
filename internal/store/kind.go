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
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind"
	readoption "github.com/relexec/rxp/kind/read/option"
	"github.com/relexec/rxp/kind/read/selector"
	writeoption "github.com/relexec/rxp/kind/write/option"
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

// kindReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Kind.
func (s *Store) kindReadValidate(
	ctx context.Context,
	sel selector.Selector,
	opts readoption.Options,
) error {
	return sel.Validate()
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
