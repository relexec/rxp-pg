package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/relexec/rxp/errors"
	rxptypes "github.com/relexec/rxp/types"

	"github.com/relexec/rxp-pg/internal/types"
)

// kindRecord returns a record with the supplied kind's row ID.
//
// This method will populate typemap caches for kind records it finds in the
// DB, however will not auto-create DB records.
func (s *Store) kindRecord(
	ctx context.Context,
	kind rxptypes.Kind,
) (*types.Record, error) {
	kinds := s.typeMap.Kinds()
	if kinds == nil {
		return nil, ErrTypeMapNotInitialized
	}

	var err error
	var found bool
	rec := &types.Record{}
	rec.RowID, found = kinds.RowID(string(kind))
	if !found {
		rec, err = s.kindRead(ctx, kind)
		if err != nil {
			return nil, err
		}
		if rec != nil {
			set := kinds.Set(string(kind), rec.RowID)
			if !set {
				return nil, errors.Internal(
					fmt.Sprintf(
						"failed setting kind cache key %q to value %d",
						kind, rec.RowID,
					),
				)
			}
		}
	}
	return rec, nil
}

// kindWrite atomically writes the supplied Kind to persistent storage,
// returning any mutated Kind along with a record containing internal DB
// information.
func (s *Store) kindWrite(
	ctx context.Context,
	kind rxptypes.Kind,
) (*types.Record, error) {
	rec := types.Record{}
	fn := func(tx pgx.Tx) error {
		qs := "INSERT INTO kinds (name) VALUES ($1) RETURNING id"
		err := tx.QueryRow(ctx, qs, string(kind)).Scan(&rec.RowID)
		if err != nil {
			return errors.Internal(
				"failed inserting kinds record",
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

// kindRead reads a Kind from persistent storage.
func (s *Store) kindRead(
	ctx context.Context,
	kind rxptypes.Kind,
) (*types.Record, error) {
	rec := types.Record{}
	fn := func(tx pgx.Tx) error {
		qs := "SELECT id FROM kinds WHERE name = $1"
		err := tx.QueryRow(ctx, qs, string(kind)).Scan(&rec.RowID)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			if err != nil {
				return errors.Internal(
					"failed reading kinds record",
					errors.WithWrap(err),
				)
			}
		}
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &rec, nil
}
