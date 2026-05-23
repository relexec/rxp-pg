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
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/namespace"

	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
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

// dbReadByUUID performs a SELECT query to return the stored namespace record
// having the supplied UUID.
func (s *Store) dbReadByUUID(
	ctx context.Context,
	uuid string,
) (*Record, error) {
	out := Record{
		Namespace: namespace.New(
			namespace.WithUUID(uuid),
		),
	}
	fn := func(tx pgx.Tx) error {
		var domainRowID int64
		var name api.NamespaceName
		qs := "SELECT domain, id, name FROM namespaces WHERE uuid = $1"
		err := tx.QueryRow(ctx, qs, uuid).Scan(&domainRowID, &out.RowID, &name)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading namespaces record",
				errors.WithWrap(err),
			)
		}
		domainRec, err := s.domainStore.ReadByRowID(ctx, domainRowID)
		if err != nil {
			return errors.Internal(
				"failed reading domain record for namespace",
				errors.WithWrap(err),
			)
		}
		out.DomainRecord = domainRec
		out.Namespace.SetName(name)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadByName performs a SELECT query to return the stored namespace record
// having the supplied Name.
func (s *Store) dbReadByName(
	ctx context.Context,
	domainRec *storedomain.Record,
	name api.NamespaceName,
) (*Record, error) {
	out := Record{
		DomainRecord: domainRec,
		Namespace: namespace.New(
			namespace.WithDomain(domainRec.Domain),
			namespace.WithName(name),
		),
	}
	fn := func(tx pgx.Tx) error {
		var uuid string
		qs := "SELECT id, uuid FROM namespaces WHERE domain = $1 AND name = $2"
		err := tx.QueryRow(ctx, qs, domainRec.RowID, name).Scan(&out.RowID, &uuid)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading namespaces record",
				errors.WithWrap(err),
			)
		}
		out.Namespace.SetUUID(uuid)
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbInsert atomically writes the supplied Namespace to persistent storage.
func (s *Store) dbInsert(
	ctx context.Context,
	domainRec *storedomain.Record,
	dom *namespace.Namespace,
) error {
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	uuid := dom.UUID()
	name := dom.Name()
	fn := func(tx pgx.Tx) error {
		qs := `
INSERT INTO namespaces (
  domain
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
			domainRec.RowID,
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
						return errors.DuplicateKey("namespace", "uuid", uuid)
					} else {
						return errors.DuplicateName("namespace", name)
					}
				}
			}
		}
		return err
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return errors.Internal(
			"failed inserting namespaces record",
			errors.WithWrap(err),
		)
	}
	return nil
}
