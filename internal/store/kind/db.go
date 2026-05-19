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
	"github.com/relexec/rxp/kind"
	"github.com/relexec/rxp/types"

	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

var (
	txOptsStrict = pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
	}
)

// dbExec executes the supplied function within the context of a database
// transaction. If the function errors or panics, a ROLLBACK is automatically
// issued for the transaction. If the function completes successfully, a COMMIT
// is automatically issued for the transaction.
func (s *Store) dbExec(
	ctx context.Context,
	fn func(tx pgx.Tx) error,
) error {
	pool := s.pool
	if pool == nil {
		return errors.Internal("connection pool not initialized")
	}
	tx, err := pool.BeginTx(ctx, txOptsStrict)
	if err != nil {
		return errors.Internal(
			fmt.Sprintf("failed beginning transaction"),
			errors.WithWrap(err),
		)
	}

	// make sure we rollback our transaction if a panic occurs.
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback(ctx)
			panic(p)
		}
	}()

	err = fn(tx)
	if err != nil {
		rbErr := tx.Rollback(ctx)
		if rbErr != nil {
			return errors.Internal(
				fmt.Sprintf("failed rolling back transaction"),
				errors.WithWrap(err),
			)
		}
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return errors.Internal(
			fmt.Sprintf("failed committing transaction"),
			errors.WithWrap(err),
		)
	}
	return nil
}

// dbReadByRowID performs a SELECT query to return the stored kind record
// having the supplied internal DB RowID.
func (s *Store) dbReadByRowID(
	ctx context.Context,
	rowID int64,
) (*Record, error) {
	out := Record{
		RowID: rowID,
	}
	fn := func(tx pgx.Tx) error {
		var systemRowID int64
		var name types.KindName
		var namescope types.Namescope
		qs := "SELECT system, name, namescope FROM kinds WHERE id = $1"
		err := tx.QueryRow(
			ctx, qs, rowID,
		).Scan(&systemRowID, &name, &namescope)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading kinds record",
				errors.WithWrap(err),
			)
		}
		systemRec, err := s.systemStore.ReadByRowID(ctx, systemRowID)
		if err != nil {
			return errors.Internal(
				"failed reading system record for kind",
				errors.WithWrap(err),
			)
		}
		out.Kind = kind.New(
			kind.WithSystem(systemRec.System),
			kind.WithName(name),
			kind.WithNamescope(namescope),
		)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByName performs a SELECT query to return the stored kind record
// having the supplied Name.
func (s *Store) dbReadByName(
	ctx context.Context,
	systemRec *storesystem.Record,
	name types.KindName,
) (*Record, error) {
	out := Record{
		Kind: kind.New(
			kind.WithSystem(systemRec.System),
			kind.WithName(name),
		),
	}
	fn := func(tx pgx.Tx) error {
		var namescope types.Namescope
		qs := "SELECT id, namescope FROM kinds WHERE system = $1 AND name = $2"
		err := tx.QueryRow(
			ctx, qs, systemRec.RowID, name,
		).Scan(&out.RowID, &namescope)
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

// dbInsert atomically writes the supplied Kind to persistent storage.
func (s *Store) dbInsert(
	ctx context.Context,
	systemRec *storesystem.Record,
	kind types.Kind,
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
			ctx, qs, systemRec.RowID,
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
