package store

import (
	"context"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/system"
)

// Write atomically writes the supplied System to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	system *system.System,
) error {
	var name *string
	uuid := system.UUID()
	sysName := system.Name()
	if sysName != "" {
		name = &sysName
	}
	fn := func(tx pgx.Tx) error {
		qs := "INSERT INTO systems (uuid, name) VALUES ($1, $2)"
		_, err := tx.Exec(ctx, qs, uuid, name)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.DuplicateKey("system", "uuid", system.UUID())
				}
			}
		}
		return err
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return errors.Internal(
			"failed inserting systems record",
			errors.WithWrap(err),
		)
	}
	return nil
}
