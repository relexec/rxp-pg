package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/relexec/rxp/api"
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/errors"

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

// dbReadByRowID performs a SELECT query to return the stored domain record
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
		var name api.DomainName
		var uuid string
		qs := "SELECT system, uuid, name FROM domains WHERE id = $1"
		err := tx.QueryRow(ctx, qs, rowID).Scan(&systemRowID, &uuid, &name)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domains record",
				errors.WithWrap(err),
			)
		}
		systemRec, err := s.systemStore.ReadByRowID(ctx, systemRowID)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading system record for domain",
				errors.WithWrap(err),
			)
		}
		out.Domain = domain.New(
			domain.WithSystem(systemRec.System),
			domain.WithUUID(uuid),
			domain.WithName(name),
		)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByUUID performs a SELECT query to return the stored domain record
// having the supplied UUID.
func (s *Store) dbReadByUUID(
	ctx context.Context,
	uuid string,
) (*Record, error) {
	out := Record{
		Domain: domain.New(
			domain.WithUUID(uuid),
		),
	}
	fn := func(tx pgx.Tx) error {
		var name api.DomainName
		qs := "SELECT id, name FROM domains WHERE uuid = $1"
		err := tx.QueryRow(ctx, qs, uuid).Scan(&out.RowID, &name)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domains record",
				errors.WithWrap(err),
			)
		}
		out.Domain.SetName(name)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByName performs a SELECT query to return the stored domain record
// having the supplied Name.
func (s *Store) dbReadByName(
	ctx context.Context,
	systemRec *storesystem.Record,
	name api.DomainName,
) (*Record, error) {
	out := Record{
		Domain: domain.New(
			domain.WithSystem(systemRec.System),
			domain.WithName(name),
		),
	}
	fn := func(tx pgx.Tx) error {
		var uuid string
		qs := "SELECT id, uuid FROM domains WHERE system = $1 AND name = $2"
		err := tx.QueryRow(ctx, qs, systemRec.RowID, name).Scan(&out.RowID, &uuid)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading domains record",
				errors.WithWrap(err),
			)
		}
		out.Domain.SetUUID(uuid)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbInsert atomically writes the supplied Domain to persistent storage.
func (s *Store) dbInsert(
	ctx context.Context,
	systemRec *storesystem.Record,
	dom *domain.Domain,
) error {
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	uuid := dom.UUID()
	name := dom.Name()
	fn := func(tx pgx.Tx) error {
		qs := `
INSERT INTO domains (
  system
, uuid
, name
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
			ctx, qs,
			systemRec.RowID,
			uuid,
			name,
			createdOn,
			createdBy,
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					conName := pgErr.ConstraintName
					if strings.Contains(conName, "uuid") {
						return errors.DuplicateKey("domain", "uuid", uuid)
					} else {
						return errors.DuplicateName("domain", name)
					}
				}
			}
		}
		return err
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return errors.Internal(
			"failed inserting domains record",
			errors.WithWrap(err),
		)
	}
	return nil
}
