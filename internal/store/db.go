package store

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	rxperrors "github.com/relexec/rxp/errors"
)

const (
	acquireTimeoutDuration = time.Millisecond * 500
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
	if s.Pool == nil {
		panic("connection pool not initialized")
	}
	// We manually acquire a connection from the pool instead of using
	// Pool.BeginTx so that we can pass in an explicit short timeout for the
	// acquire operation.
	acqCtx, acqCancel := context.WithTimeout(ctx, acquireTimeoutDuration)
	defer acqCancel()

	conn, err := s.Pool.Acquire(acqCtx)
	if err != nil {
		return rxperrors.Internal(
			"failed acquiring connection from pool",
			rxperrors.WithWrap(err),
		)
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, txOptsStrict)
	if err != nil {
		return rxperrors.Internal(
			"failed beginning transaction",
			rxperrors.WithWrap(err),
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
			return rxperrors.Internal(
				"failed rolling back transaction",
				rxperrors.WithWrap(rbErr),
			)
		}
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return rxperrors.Internal(
			"failed committing transaction",
			rxperrors.WithWrap(err),
		)
	}
	return nil
}
