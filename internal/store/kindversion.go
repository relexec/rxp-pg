package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/relexec/rxp/errors"
	rxptypes "github.com/relexec/rxp/types"

	"github.com/relexec/rxp-pg/internal/types"
)

// kindVersionRecord returns a record with the supplied kind and version's row
// ID.
//
// This method will populate typemap caches for kind and kindVersion records it
// finds in the DB, however will not auto-create DB records.
func (s *Store) kindVersionRecord(
	ctx context.Context,
	kv rxptypes.KindVersion,
) (*types.Record, error) {
	kindVersions := s.typeMap.KindVersions()
	if kindVersions == nil {
		return nil, ErrTypeMapNotInitialized
	}

	kind := kv.Kind()

	kindRec, err := s.kindRecord(ctx, kind)
	if err != nil {
		return nil, err
	}

	var found bool
	rec := &types.Record{}
	rec.RowID, found = kindVersions.RowID(string(kv))
	if !found {
		rec, err = s.kindVersionRead(ctx, kindRec.RowID, kv.VersionString())
		if err != nil {
			return nil, err
		}
		if rec != nil {
			set := kindVersions.Set(string(kv), rec.RowID)
			if !set {
				return nil, errors.Internal(
					fmt.Sprintf(
						"failed setting kindversion cache key %q to value %d",
						kv, rec.RowID,
					),
				)
			}
		}
	}
	return rec, nil
}

// kindVersionWrite atomically writes the supplied KindVersion to persistent storage,
// returning any mutated KindVersion along with a record containing internal DB
// information.
func (s *Store) kindVersionWrite(
	ctx context.Context,
	kindID int64,
	version string,
) (*types.Record, error) {
	rec := types.Record{}
	fn := func(tx pgx.Tx) error {
		qs := "INSERT INTO kind_versions (kind, version) VALUES ($1, $2) RETURNING id"
		err := tx.QueryRow(ctx, qs, kindID, version).Scan(&rec.RowID)
		if err != nil {
			return errors.Internal(
				"failed inserting kind_versions record",
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

// kindVersionRead reads a KindVersion from persistent storage.
func (s *Store) kindVersionRead(
	ctx context.Context,
	kindID int64,
	version string,
) (*types.Record, error) {
	rec := types.Record{}
	fn := func(tx pgx.Tx) error {
		qs := "SELECT id FROM kind_versions WHERE kind = $1 AND version = $2"
		err := tx.QueryRow(ctx, qs, kindID, version).Scan(&rec.RowID)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading kind_versions record",
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

// kindVersionEnsure ensures that there is a kind and kindversion record in
// persistent storage matching the supplied kind and version string. This
// method is called when creating new metas when we do not have a record of the
// meta's kindversion yet.
//
// Returns a Record with the kindversion's row ID.
func (s *Store) kindVersionEnsure(
	ctx context.Context,
	kind rxptypes.Kind,
	version string,
) (*types.Record, error) {
	// we need to create a new kind version and we need the kind record
	// ID to do that. there may be an existing kind record. if not,
	// create one.
	kRec, err := s.kindRecord(ctx, kind)
	if err != nil {
		if err != errors.ErrNotFound {
			return nil, err
		}
		kRec, err = s.kindWrite(ctx, kind)
		if err != nil {
			return nil, err
		}
	}
	return s.kindVersionWrite(ctx, kRec.RowID, version)
}
