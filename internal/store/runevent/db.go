package store

import (
	"context"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	apirun "github.com/relexec/rxp/api/run"
	"github.com/relexec/rxp/errors"
)

var (
	runEventColumns = []string{
		"run",
		"sequence",
		"event_type",
		"occurred_on",
	}
)

// dbInsert is called to atomically write run event records.
func (s *Store) dbInsert(
	ctx context.Context,
	run apirun.Run,
	events []apirun.Event,
) error {
	runRowID := run.SystemInternalIDInt64()
	fn := func(tx pgx.Tx) error {
		_, err := tx.CopyFrom(
			ctx,
			pgx.Identifier{"run_events"},
			runEventColumns,
			pgx.CopyFromSlice(len(events), func(x int) ([]any, error) {
				return []any{
					runRowID,
					events[x].Sequence,
					int(events[x].Type),
					events[x].On.UnixNano(),
				}, nil
			}),
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					// This will be the run ID and sequence key violation,
					// which indicates that another thread has attempts to
					// write these same run event records.
					return errors.ErrConflict
				}
			}
			return errors.Internal(
				"failed inserting run events records using COPY protocol",
				errors.WithWrap(err),
			)
		}
		return nil
	}
	return s.Exec(ctx, fn)
}
