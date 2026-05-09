package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/meta"
	"github.com/relexec/rxp/meta/read"
	readoption "github.com/relexec/rxp/meta/read/option"
	"github.com/relexec/rxp/meta/read/selector"
	"github.com/relexec/rxp/meta/schema"
	"github.com/relexec/rxp/meta/write"
	writeoption "github.com/relexec/rxp/meta/write/option"
	"github.com/relexec/rxp/metrics"
	rxptypes "github.com/relexec/rxp/types"
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

	kv = sel.KindVersion()
	system := sel.System()
	if system == nil {
		system = s.hostSystem.System
	}

	entry, err := s.metaRead(ctx, system, kv)
	if err != nil {
		return nil, err
	}
	return entry.Meta, nil
}

// MetaWrite persists the single supplied Meta to backend storage.
func (s *Store) MetaWrite(
	ctx context.Context,
	meta rxptypes.Meta,
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
			metrics.AttributeTargetType(metrics.TargetTypeMeta),
			metrics.AttributeKindVersion(meta.KindVersion()),
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
	err = s.metaWriteValidate(ctx, meta, wopts)
	if err != nil {
		return err
	}

	system := meta.System()
	if system == nil {
		system = s.hostSystem.System
	}

	systemEntry, err := s.systemRead(ctx, system.UUID())
	if err != nil {
		return err
	}
	return s.metaDBWrite(ctx, systemEntry, meta)
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

// metaWriteValidate returns an error if the supplied meta and write options
// are not valid for writing a single Meta.
func (s *Store) metaWriteValidate(
	ctx context.Context,
	meta rxptypes.Meta,
	opts writeoption.Options,
) error {
	return meta.Validate()
}

// metaRead returns a metaEntry for the supplied pre-validated system and
// kindversion.  This method will populate any caches with any read records.
func (s *Store) metaRead(
	ctx context.Context,
	system rxptypes.System,
	kv rxptypes.KindVersion,
) (*metaEntry, error) {
	cacheKey := newMetaCacheKey(system, kv)
	cached, found := s.metaCacheRead(ctx, cacheKey)
	if found {
		return cached, nil
	}

	systemEntry, err := s.systemRead(ctx, system.UUID())
	if err != nil {
		return nil, err
	}

	entry, err := s.metaDBRead(ctx, systemEntry, kv)
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

// metaCacheWrite writes the supplied cache entry if the meta cache is enabled.
func (s *Store) metaCacheWrite(
	ctx context.Context,
	key metaCacheKey,
	entry *metaEntry,
) error {
	if s.metaCache == nil {
		return nil
	}
	set := s.metaCache.Set(key, entry)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting meta cache key %q", key),
		)
	}
	return nil
}

// metaDBRead performs a SELECT query to return the stored meta record.
func (s *Store) metaDBRead(
	ctx context.Context,
	systemEntry *systemEntry,
	kv rxptypes.KindVersion,
) (*metaEntry, error) {
	kind := kv.Kind()
	ver := kv.VersionString()
	out := metaEntry{
		SystemRowID: systemEntry.RowID,
	}
	var namescope rxptypes.Namescope
	var schemaBytes sql.NullString
	var schema schema.Schema
	fn := func(tx pgx.Tx) error {
		qs := "SELECT id, namescope, schema FROM metas WHERE system = $1 AND kind = $2 AND version = $3"
		err := tx.QueryRow(ctx, qs, systemEntry.RowID, kind, ver).Scan(
			&out.RowID,
			&namescope,
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
			meta.WithKindVersion(kv),
			meta.WithNamescope(namescope),
			meta.WithSchema(&schema),
		)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// metaDBWrite inserts the supplied meta information into the database.
func (s *Store) metaDBWrite(
	ctx context.Context,
	systemEntry *systemEntry,
	meta rxptypes.Meta,
) error {
	kv := meta.KindVersion()
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	schemaJSON, err := meta.SchemaJSON()
	if err != nil {
		return err
	}
	fn := func(tx pgx.Tx) error {
		qs := "INSERT INTO metas (system, kind, version, namescope, schema, last_modified_on, last_modified_by) VALUES ($1, $2, $3, $4, $5, $6, $7)"
		_, err := tx.Exec(
			ctx, qs,
			systemEntry.RowID,
			kv.Kind(),
			kv.VersionString(),
			meta.Namescope(),
			schemaJSON,
			createdOn,
			createdBy,
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.ExpectedNotToExist(kv)
				}
			}
			return errors.Internal(
				"failed inserting objects record",
				errors.WithWrap(err),
			)
		}
		return nil
	}
	return s.dbExec(ctx, fn)
}

/*
// metaWrite atomically writes the supplied Meta to persistent storage,
// returning any mutated Meta.
func (s *Store) metaWrite(

	ctx context.Context,
	opts write.Options,
	meta rxptypes.Meta,

	) error {
		kv := meta.KindVersion()
		kind := kv.Kind()
		schemaJSON, err := meta.SchemaJSON()
		if err != nil {
			return err
		}

		expectGeneration := opts.Generation()

		var kvRec *types.Record
		var delta *cmp.Delta
		var deltaJSON string
		fn := func(tx pgx.Tx) error {
			kvRec, err = s.kindVersionRecord(ctx, kv)
			if err != nil {
				if err != errors.ErrNotFound {
					return err
				}
				// If the caller specified a generation in the options, that means
				// the caller expects that the kindversion should already exist.
				if expectGeneration > 0 {
					return errors.ExpectedToExist(kv)
				}

				// No kindversion exists and caller expected this. Let's ensure
				// that the kindversion is created.
				kvRec, err = s.kindVersionEnsure(ctx, kind, kv.VersionString())
				if err != nil {
					return errors.Internal(
						fmt.Sprintf("failed ensuring kind version %q", kv),
						errors.WithWrap(err),
					)
				}
			} else {
				// If the caller did not specify a generation in the options, that
				// means the caller expects that the kindversion should not already
				// exist.
				if expectGeneration == 0 {
					return errors.ExpectedNotToExist(kv)
				}
			}

			metaRec, err := s.metaRecord(ctx, kv)
			if err != nil {
				if err != errors.ErrNotFound {
					return err
				}
				// If the caller specified a generation in the options, that means
				// the caller expects that there should already exist a meta with
				// this kindversion and a matching generation.
				if expectGeneration > 0 {
					return errors.ExpectedToExist(kv)
				}

				// the delta for a new meta can be generated by Diff()'ing the
				// meta against [cmp.Zero].
				delta, err = meta.Diff(cmp.Zero)
				if err != nil {
					return errors.New(
						"failed diffing with empty meta",
						errors.WithCode(errors.ErrCodeBadRequest),
						errors.WithWrap(err),
					)
				}
				if !delta.Different() {
					return errors.New(
						"expected difference between empty meta and supplied meta",
						errors.WithCode(errors.ErrCodeBadRequest),
						errors.WithWrap(err),
					)
				}
				deltaJSONBytes, err := delta.MarshalJSON()
				if err != nil {
					return errors.Internal(
						"failed marshaling delta",
						errors.WithWrap(err),
					)
				}
				deltaJSON = string(deltaJSONBytes)

				metaGeneration := int64(1)
				return s.metaInsert(
					ctx, tx,
					kvRec.RowID,
					meta.Namescope(),
					schemaJSON, deltaJSON,
					metaGeneration,
				)
			} else {
				// Caller expects that the latest generation of this meta is exactly
				// what they specified.
				if expectGeneration != rxptypes.Generation(metaRec.Generation) {
					return errors.ExpectedGeneration(
						kv, expectGeneration, metaRec.Generation,
					)
				}
				ropts := []read.Option{}
				if expectGeneration > 0 {
					ropts = append(ropts, read.HavingGeneration(expectGeneration))
				}

				ro := read.NewOptions(ropts...)
				existingMetaRec, existingMeta, err := s.metaRead(
					ctx, kv, *ro, kvRec.RowID,
				)
				if err != nil {
					return errors.Internal("failed reading existing meta")
				}
				delta, err = meta.Diff(existingMeta)
				if err != nil {
					return errors.New(
						"failed diffing with existing meta",
						errors.WithCode(errors.ErrCodeBadRequest),
						errors.WithWrap(err),
					)
				}
				if !delta.Different() {
					return errors.New(
						"expected difference between existing "+
							"meta and supplied meta",
						errors.WithCode(errors.ErrCodeBadRequest),
						errors.WithWrap(err),
					)
				}
				deltaJSONBytes, err := delta.MarshalJSON()
				if err != nil {
					return errors.Internal(
						"failed marshaling delta",
						errors.WithWrap(err),
					)
				}
				deltaJSON = string(deltaJSONBytes)

				return s.metaInsert(
					ctx, tx,
					kvRec.RowID,
					meta.Namescope(),
					schemaJSON, deltaJSON,
					existingMetaRec.Generation,
				)
			}
		}
		return s.dbExec(ctx, fn)
	}

// metaInsert creates a new meta and meta_generation record with the supplied
// field values.
func (s *Store) metaInsert(

	ctx context.Context,
	tx pgx.Tx,
	kindVersionID int64,
	namescope rxptypes.Namescope,
	schemaJSON string,
	deltaJSON string,
	metaGeneration int64,

	) error {
		createdBy := rxpcontext.Identity(ctx)
		createdOn := time.Now().UnixNano()
		var rowID int64
		qs := "INSERT INTO metas (kind_version, namescope, schema, generation) VALUES ($1, $2, $3, $4) RETURNING id"
		err := tx.QueryRow(
			ctx, qs,
			kindVersionID,
			namescope,
			schemaJSON,
			metaGeneration,
		).Scan(&rowID)
		if err != nil {
			return errors.Internal(
				"failed inserting metas record",
				errors.WithWrap(err),
			)
		}
		qs = "INSERT INTO meta_generations (meta, generation, delta, created_on, created_by) VALUES ($1, $2, $3, $4, $5)"
		_, err = tx.Exec(
			ctx, qs,
			rowID,
			metaGeneration,
			deltaJSON,
			createdOn,
			createdBy,
		)
		if err != nil {
			return errors.Internal(
				"failed inserting meta_generations record",
				errors.WithWrap(err),
			)
		}
		return nil
	}

// metaRecordReadLatest reads a Meta's row ID and latest generation from
// persistent storage.
func (s *Store) metaRecordReadLatest(

	ctx context.Context,
	kindVersionID int64,

	) (*types.Record, error) {
		rec := types.Record{}
		fn := func(tx pgx.Tx) error {
			qs := "SELECT id, generation FROM metas WHERE kind_version = $1"
			err := tx.QueryRow(
				ctx, qs, kindVersionID,
			).Scan(&rec.RowID, &rec.Generation)
			if err != nil {
				if err == pgx.ErrNoRows {
					return errors.ErrNotFound
				}
				return errors.Internal(
					"failed reading metas record",
					errors.WithWrap(err),
				)
			}
			return nil
		}
		if err := s.dbExec(ctx, fn); err != nil {
			return nil, err
		}
		return &rec, nil
	}

// metaRead returns a Record and Meta given a kindVersion DB row ID.
func (s *Store) metaRead(

	ctx context.Context,
	kv rxptypes.KindVersion,
	opts read.Options,
	kindVersionID int64,

	) (*types.Record, rxptypes.Meta, error) {
		var rec types.Record
		var res rxptypes.Meta
		var namescope rxptypes.Namescope
		var schemaBytes sql.NullString
		var schema jsonschema.Schema
		wantGen := int64(opts.Generation())

		fn := func(tx pgx.Tx) error {
			// We only store the actual Meta record for the latest generation. For older
			// generations, we construct the returned Meta record by reading the deltas
			// from previous generations and reconstructing the Meta up to the requested
			// generation.
			qs := "SELECT id, generation, namescope, schema FROM metas WHERE kind_version = $1"
			err := tx.QueryRow(
				ctx, qs, kindVersionID,
			).Scan(&rec.RowID, &rec.Generation, &namescope, &schemaBytes)
			if err != nil {
				if err == pgx.ErrNoRows {
					return errors.ErrNotFound
				}
				return errors.Internal(
					"failed reading metas record",
					errors.WithWrap(err),
				)
			}
			if wantGen == 0 || wantGen == rec.Generation {
				// Requested generation is the latest generation, so we can
				// construct the Meta from the data in the meta table.
				sv, err := kv.Version()
				if err != nil {
					return errors.Internal(
						"failed getting kindversion version",
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
				res = meta.New(
					meta.WithKind(kv.Kind()),
					meta.WithVersion(*sv),
					meta.WithNamescope(namescope),
					meta.WithSchema(&schema),
					meta.WithGeneration(rxptypes.Generation(rec.Generation)),
				)
				return nil
			}
			if wantGen > rec.Generation {
				return errors.ErrNotFound
			}
			// Need to read previous generations and construct the returned Meta by
			// examining the deltas.
			// TODO
			return nil
		}
		if err := s.dbExec(ctx, fn); err != nil {
			return nil, nil, err
		}
		if wantGen != 0 {
			rec.Generation = int64(wantGen)
		}
		return &rec, res, nil
	}
*/
var _ read.MetaReader = (*Store)(nil)
var _ write.MetaWriter = (*Store)(nil)
