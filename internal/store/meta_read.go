package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/jackc/pgx/v5"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/meta"
	"github.com/relexec/rxp/meta/read"
	readoption "github.com/relexec/rxp/meta/read/option"
	"github.com/relexec/rxp/meta/read/selector"
	"github.com/relexec/rxp/meta/schema"
	"github.com/relexec/rxp/metrics"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/relexec/rxp/version"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type metaCacheKey string

func (k metaCacheKey) SystemUUID() string {
	parts := strings.Split(string(k), "|")
	return parts[0]
}

func (k metaCacheKey) KindVersion() rxptypes.KindVersion {
	parts := strings.Split(string(k), "|")
	return rxptypes.KindVersion(parts[1])
}

// metaEntry decorates a Meta with internal DB information.
type metaEntry struct {
	// RowID is the internal database SERIAL for the metas record.
	RowID int64
	// SystemRowID is the internal database SERIAL for the meta's associated
	// system record.
	SystemRowID int64
	// KindRowID is the internal database SERIAL for the meta's associated kind
	// record.
	KindRowID int64
	// Meta is the publicly-exposed Meta object.
	Meta *meta.Meta
}

func newMetaCacheKey(
	system rxptypes.System,
	kv rxptypes.KindVersion,
) metaCacheKey {
	return metaCacheKey(system.UUID() + "|" + string(kv))
}

// MetaRead reads a single Meta from persistent storage.
func (s *Store) MetaRead(
	ctx context.Context,
	sel selector.Selector,
	opts ...readoption.Option,
) (rxptypes.Meta, error) {
	err := s.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	var kv rxptypes.KindVersion

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeTargetType(metrics.TargetTypeMeta),
			metrics.AttributeKindVersion(kv),
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
	err = s.metaReadValidate(ctx, sel, ropts)
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

	kv = sel.KindVersion()
	kindEntry, err := s.kindRead(ctx, systemEntry, kv.Kind())
	if err != nil {
		return nil, errors.ErrKindVersionUnknown
	}

	entry, err := s.metaRead(ctx, systemEntry, kindEntry, kv)
	if err != nil {
		return nil, err
	}
	return entry.Meta, nil
}

// metaReadValidate returns an error if the supplied selector and read options
// are not valid for reading a single Meta.
func (s *Store) metaReadValidate(
	ctx context.Context,
	sel selector.Selector,
	opts readoption.Options,
) error {
	return sel.Validate()
}

// metaRead returns a metaEntry for the supplied pre-validated system entry,
// kind entry and kindversion.  This method will populate any caches with any
// read records.
func (s *Store) metaRead(
	ctx context.Context,
	systemEntry *systemEntry,
	kindEntry *kindEntry,
	kv rxptypes.KindVersion,
) (*metaEntry, error) {
	system := systemEntry.System
	cacheKey := newMetaCacheKey(system, kv)
	cached, found := s.metaCacheRead(ctx, cacheKey)
	if found {
		return cached, nil
	}

	entry, err := s.metaDBRead(ctx, systemEntry, kindEntry, kv)
	if err != nil {
		return nil, err
	}
	err = s.metaCacheWrite(ctx, cacheKey, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

// metaCacheRead returns a cached Meta and whether or not the supplied meta
// was found in the cache.
func (s *Store) metaCacheRead(
	ctx context.Context,
	key metaCacheKey,
) (*metaEntry, bool) {
	if s.metaCache == nil {
		return nil, false
	}
	return s.metaCache.Get(key)
}

// metaDBRead performs a SELECT query to return the stored meta record.
func (s *Store) metaDBRead(
	ctx context.Context,
	systemEntry *systemEntry,
	kindEntry *kindEntry,
	kv rxptypes.KindVersion,
) (*metaEntry, error) {
	sv, _ := kv.Version()
	verStr := kv.VersionString()
	out := metaEntry{
		SystemRowID: systemEntry.RowID,
	}
	var schemaBytes sql.NullString
	var schema schema.Schema
	fn := func(tx pgx.Tx) error {
		qs := "SELECT id, schema FROM metas WHERE system = $1 AND kind = $2 AND version = $3"
		err := tx.QueryRow(ctx, qs, systemEntry.RowID, kindEntry.RowID, verStr).Scan(
			&out.RowID,
			&schemaBytes,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading metas record",
				errors.WithWrap(err),
			)
		}
		if schemaBytes.Valid {
			err = json.Unmarshal([]byte(schemaBytes.String), &schema)
			if err != nil {
				return errors.Internal(
					"failed unmarshaling meta schema",
					errors.WithWrap(err),
				)
			}
		}
		out.Meta = meta.New(
			meta.WithKind(kindEntry.Kind),
			meta.WithVersion(*sv),
			meta.WithSchema(&schema),
		)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// metaDBVersionsForKind returns a version.Set representing all the semantic
// versions known for the supplied Kind.
func (s *Store) metaDBVersionsForKind(
	ctx context.Context,
	systemEntry *systemEntry,
	kindEntry *kindEntry,
) (version.Set, error) {
	var versionStrs []string
	fn := func(tx pgx.Tx) error {
		qs := "SELECT version FROM metas WHERE system = $1 AND kind = $2"
		rows, err := tx.Query(ctx, qs, systemEntry.RowID, kindEntry.RowID)
		if err != nil {
			return errors.Internal(
				"failed reading meta records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		versionStrs, err = pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return errors.Internal(
				"failed collecting meta.versions",
				errors.WithWrap(err),
			)
		}

		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	versions := []semver.Version{}
	for _, verStr := range versionStrs {
		v, err := semver.NewVersion(verStr)
		if err != nil {
			return nil, errors.Internal(
				"failed parsing semver",
				errors.WithWrap(err),
			)
		}
		versions = append(versions, *v)
	}
	vs := version.Set{}
	vs.Add(versions...)
	return vs, nil
}

var _ read.MetaReader = (*Store)(nil)
