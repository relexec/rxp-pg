package store

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/relexec/rxp/errors"
)

var (
	txOptsStrict = pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
	}
)

// Exec executes the supplied function within the context of a database
// transaction. If the function errors or panics, a ROLLBACK is automatically
// issued for the transaction. If the function completes successfully, a COMMIT
// is automatically issued for the transaction.
func (s Store) Exec(
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
			rbErr := tx.Rollback(ctx)
			if rbErr != nil && rbErr != pgx.ErrTxClosed {
				fmt.Fprintf(
					os.Stderr,
					"rollback failed during panic recovery: %s",
					rbErr.Error(),
				)
			}
			panic(p)
		}
	}()

	err = fn(tx)
	if err != nil {
		rbErr := tx.Rollback(ctx)
		if rbErr != nil && rbErr != pgx.ErrTxClosed {
			return errors.Internal(
				fmt.Sprintf("failed rolling back transaction"),
				errors.WithWrap(rbErr),
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
