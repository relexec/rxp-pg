package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/relexec/rxp/api"
	apirun "github.com/relexec/rxp/api/run"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/query"

	storeobject "github.com/relexec/rxp-pg/internal/store/object"
)

// dbReadByRowID returns the run record having the supplied
// internal DB RowID.
func (s *Store) dbReadByRowID(
	ctx context.Context,
	rowID int64,
) (*apirun.Run, error) {
	var uuid string
	var targetUUID string
	var targetGeneration api.Generation
	var rootRowID int64
	var requestedOn int64
	var callerIdentity string
	var callerSystemID int64
	var callerDomainID sql.NullInt64
	var inVars sql.NullString
	var runOptions sql.NullString
	var scheduledOn int64
	var startedOn sql.NullInt64
	var completedOn sql.NullInt64
	var failedOn sql.NullInt64
	var pausedOn sql.NullInt64
	var resumedOn sql.NullInt64
	var canceledOn sql.NullInt64

	out := &apirun.Run{}
	out.SetSystemInternalID(rowID)

	qargs := []any{rowID}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  r.uuid AS run_uuid
, t.uuid AS target_uuid
, t.generation AS target_generation
, r.root AS root_id
, rr.created_on AS requested_on
, rr.caller_identity AS caller_identity
, rr.caller_system_id AS caller_system_id
, rr.caller_domain_id AS caller_domain_id
, rr.in_vars AS in_vars
, rr.options AS options
, r.scheduled_on
, r.started_on
, r.completed_on
, r.failed_on
, r.paused_on
, r.resumed_on
, r.canceled_on
FROM runs AS r
INNER JOIN run_requests AS rr
 ON r.id = rr.run
INNER JOIN object_generations AS t
 ON r.target = t.id
WHERE r.id = $1
`
		err := tx.QueryRow(
			ctx, qs, qargs...,
		).Scan(
			&uuid,
			&targetUUID,
			&targetGeneration,
			&rootRowID,
			&requestedOn,
			&callerIdentity,
			&callerSystemID,
			&callerDomainID,
			&inVars,
			&runOptions,
			&scheduledOn,
			&startedOn,
			&completedOn,
			&failedOn,
			&pausedOn,
			&resumedOn,
			&canceledOn,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading runs record by row ID",
				errors.WithWrap(err),
			)
		}
		caller := api.Caller{
			Identity: callerIdentity,
		}
		target := apirun.Target{
			UUID:       targetUUID,
			Generation: targetGeneration,
		}
		rr := apirun.Request{
			UUID:   uuid,
			Target: target,
			Caller: caller,
			On:     time.Unix(0, requestedOn),
		}
		if runOptions.Valid {
			rr.Options = runOptions.String
		}
		if inVars.Valid {
			rr.In = inVars.String
		}
		out.SetRequest(rr)
		if startedOn.Valid {
			out.SetStartedOn(time.Unix(0, startedOn.Int64))
		}
		if completedOn.Valid {
			out.SetCompletedOn(time.Unix(0, completedOn.Int64))
		}
		if failedOn.Valid {
			out.SetFailedOn(time.Unix(0, failedOn.Int64))
		}
		if pausedOn.Valid {
			out.SetPausedOn(time.Unix(0, pausedOn.Int64))
		}
		if resumedOn.Valid {
			out.SetResumedOn(time.Unix(0, resumedOn.Int64))
		}
		if canceledOn.Valid {
			out.SetCanceledOn(time.Unix(0, canceledOn.Int64))
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return out, nil
}

/*

type runIdentifieRecord struct {
	ID       int64         `db:"run_id"`
	UUID     string        `db:"run_uuid"`
	ParentID sql.NullInt64 `db:"parent_id"`
}

// dbReadIdentifiersByRoot returns the RunIdentiifers struct containing the
// tree of Run identifiers for the supplied root row ID.
func (s *Store) dbReadIdentifiersByRoot(
	ctx context.Context,
	rootRowID int64,
) (*apirun.Identifiers, error) {

	out := &apirun.Identifiers{}
	out.SetSystemInternalID(rootRowID)

	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  r.id AS run_id
, r.uuid AS run_uuid
, r.parent AS parent_id
FROM runs AS r
WHERE r.root_id = $1
ORDER BY r.id
`
		rows, err := tx.Query(ctx, qs, rootRowID)
		if err != nil {
			return errors.Internal(
				"failed reading run identifier records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[runIdentifierRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting run identifier records",
				errors.WithWrap(err),
			)
		}
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}

	rowIDToRunIDs := make(map[int64]*apirun.RunIdentifiers, len(recs))

	for _, rec := range recs {
		runIDs := &apirun.Identifiers{UUID: rec.UUID}
		runIDs.SetSystemInternalID(rec.ID)
		runIDs.Root = out

		if rec.ParentID.Valid {
			parRowID := rec.ParentID.Int64
			parentIDs, ok := rowIDToRunIDs[parRowID]
			if !ok {
				// Because we used ORDER BY r.id in the query, failure to find
				// a parent by row ID means that a parent Run was created
				// *after* its child, which isn't logical.
				msg := fmt.Sprintf(
					"parent run record with row ID %d created after child "+
						"run record with row ID %d and UUID %q",
					parRowID, rec.ID, rec.UUID,
				)
				return nil, errors.Internal(msg)
			}
			runIDs.Parent = parentIDs
		}

		rowIDToRunIDs[rec.ID] = runIDs
	}
	return out, nil
}
*/

// dbReadByUUID returns the run record having the supplied run UUID.
func (s *Store) dbReadByUUID(
	ctx context.Context,
	uuid string,
) (*apirun.Run, error) {
	var rowID int64
	var targetUUID string
	var targetGeneration api.Generation
	var rootRowID int64
	var requestedOn int64
	var callerIdentity string
	var callerSystemID int64
	var callerDomainID sql.NullInt64
	var inVars sql.NullString
	var runOptions sql.NullString
	var scheduledOn int64
	var startedOn sql.NullInt64
	var completedOn sql.NullInt64
	var failedOn sql.NullInt64
	var pausedOn sql.NullInt64
	var resumedOn sql.NullInt64
	var canceledOn sql.NullInt64
	out := apirun.Run{}
	qargs := []any{uuid}
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  r.id AS run_id
, o.uuid AS target_uuid
, t.generation AS target_generation
, r.root AS root_id
, rr.created_on AS requested_on
, rr.caller_identity AS caller_identity
, rr.caller_system AS caller_system_id
, rr.caller_domain AS caller_domain_id
, rr.in_vars AS in_vars
, rr.options AS options
, r.scheduled_on
, r.started_on
, r.completed_on
, r.failed_on
, r.paused_on
, r.resumed_on
, r.canceled_on
FROM runs AS r
INNER JOIN run_requests AS rr
 ON r.id = rr.run
INNER JOIN object_generations AS t
 ON r.target = t.id
INNER JOIN objects AS o
 ON t.object = o.id
WHERE r.uuid = $1
`
		err := tx.QueryRow(
			ctx, qs, qargs...,
		).Scan(
			&rowID,
			&targetUUID,
			&targetGeneration,
			&rootRowID,
			&requestedOn,
			&callerIdentity,
			&callerSystemID,
			&callerDomainID,
			&inVars,
			&runOptions,
			&scheduledOn,
			&startedOn,
			&completedOn,
			&failedOn,
			&pausedOn,
			&resumedOn,
			&canceledOn,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading runs record by UUID",
				errors.WithWrap(err),
			)
		}
		caller := api.Caller{
			Identity: callerIdentity,
		}
		target := apirun.Target{
			UUID:       targetUUID,
			Generation: targetGeneration,
		}
		rr := apirun.Request{
			UUID:   uuid,
			Target: target,
			Caller: caller,
			On:     time.Unix(0, requestedOn),
		}
		if runOptions.Valid {
			rr.Options = runOptions.String
		}
		if inVars.Valid {
			rr.In = inVars.String
		}
		out.SetSystemInternalID(rowID)
		out.SetRequest(rr)
		if startedOn.Valid {
			out.SetStartedOn(time.Unix(0, startedOn.Int64))
		}
		if completedOn.Valid {
			out.SetCompletedOn(time.Unix(0, completedOn.Int64))
		}
		if failedOn.Valid {
			out.SetFailedOn(time.Unix(0, failedOn.Int64))
		}
		if pausedOn.Valid {
			out.SetPausedOn(time.Unix(0, pausedOn.Int64))
		}
		if resumedOn.Valid {
			out.SetResumedOn(time.Unix(0, resumedOn.Int64))
		}
		if canceledOn.Valid {
			out.SetCanceledOn(time.Unix(0, canceledOn.Int64))
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// dbReadObjectGenerationRowID returns the object_generations table row ID for the
// supplied Object record.
func (s *Store) dbReadObjectGenerationRowID(
	ctx context.Context,
	tx pgx.Tx,
	targetRec storeobject.Record,
) (int64, error) {
	var rowID int64
	qs := `
SELECT id FROM object_generations
WHERE object = $1 AND generation = $2
`
	err := tx.QueryRow(
		ctx, qs, targetRec.RowID, targetRec.Object.Generation,
	).Scan(&rowID)
	if err != nil {
		return -1, err
	}
	return rowID, nil
}

// dbInsert is called to create a new run record.
func (s *Store) dbInsert(
	ctx context.Context,
	targetRec storeobject.Record,
	callerSysRec *api.System,
	callerDomRec *api.Domain,
	root *apirun.Identifiers,
	parent *apirun.Identifiers,
	run apirun.Run,
) (*apirun.Run, error) {
	rr := run.Request()
	uuid := rr.UUID
	createdOn := rr.On.UnixNano()
	caller := rr.Caller
	var callerDomRowID *int64
	if callerDomRec != nil {
		tmp := callerDomRec.SystemInternalIDInt64()
		callerDomRowID = &tmp
	}

	var rootRowID int64 = -1
	if root != nil {
		rootRowID = root.SystemInternalIDInt64()
	}

	var parentRowID *int64
	if parent != nil {
		tmp := parent.SystemInternalIDInt64()
		parentRowID = &tmp
	}
	scheduledOn := run.ScheduledOn().UnixNano()

	fn := func(tx pgx.Tx) error {

		// Get the object_generations row ID to use as the target column value.
		targetRowID, err := s.dbReadObjectGenerationRowID(ctx, tx, targetRec)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading object_generations row id by "+
					"object row id and generation",
				errors.WithWrap(err),
			)
		}

		var runRowID int64
		qs := `
INSERT INTO runs (
  uuid
, target
, root
, parent
, scheduled_on
) VALUES (
  $1
, $2
, $3
, $4
, $5
) RETURNING id`
		err = tx.QueryRow(
			ctx, qs,
			uuid,
			targetRowID,
			rootRowID,
			parentRowID,
			scheduledOn,
		).Scan(&runRowID)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					// This will be the UUID column uniqueness constraint
					// violation, which indicates that another thread has
					// already created a Run with that request UUID.
					return errors.DuplicateKey("run", "uuid", uuid)
				}
			}
			return errors.Internal(
				"failed inserting runs record",
				errors.WithWrap(err),
			)
		}
		// If we are the root, we need to set the runs.root field to the
		// returned last inserted SERIAL value.
		if rootRowID == -1 {
			qs = `UPDATE runs SET root = $1 WHERE id = $2`
			_, err = tx.Exec(ctx, qs, runRowID, runRowID)
			if err != nil {
				return errors.Internal(
					"failed updating runs record with root row id",
					errors.WithWrap(err),
				)
			}
		}
		callerSysRowID := callerSysRec.SystemInternalIDInt64()
		// Now store the caller, options and input parameters in the
		// run_requests table.
		qs = `
INSERT INTO run_requests (
  run
, created_on
, caller_identity
, caller_system
, caller_domain
, options
, in_vars
) VALUES (
  $1
, $2
, $3
, $4
, $5
, $6
, $7
)`
		_, err = tx.Exec(
			ctx, qs,
			runRowID,
			createdOn,
			caller.Identity,
			callerSysRowID,
			callerDomRowID,
			rr.Options,
			rr.In,
		)
		if err != nil {
			return errors.Internal(
				"failed inserting run_requests record",
				errors.WithWrap(err),
			)
		}
		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	out := run
	return &out, nil
}

type runRecord struct {
	ID               int64          `db:"run_id"`
	UUID             string         `db:"run_uuid"`
	TargetUUID       string         `db:"target_uuid"`
	TargetGeneration api.Generation `db:"target_generation"`
	RootID           int64          `db:"root_id"`
	ParentID         sql.NullInt64  `db:"parent_id"`
	CreatedOn        int64          `db:"created_on"`
	ScheduledOn      int64          `db:"scheduled_on"`
	CallerIdentity   string         `db:"caller_identity"`
	CallerSystemID   int64          `db:"caller_system_id"`
	Options          sql.NullString `db:"options"`
	InVars           sql.NullString `db:"in_vars"`
	StartedOn        sql.NullInt64  `db:"started_on"`
	CompletedOn      sql.NullInt64  `db:"completed_on"`
	FailedOn         sql.NullInt64  `db:"failed_on"`
	PausedOn         sql.NullInt64  `db:"paused_on"`
	ResumedOn        sql.NullInt64  `db:"resumed_on"`
	CanceledOn       sql.NullInt64  `db:"canceled_on"`
	FinalizedOn      sql.NullInt64  `db:"finalized_on"`
}

// dbReadByExpression queries zero or more Runs from persistent storage given
// the pre-validated expression and options.
func (s *Store) dbReadByExpression(
	ctx context.Context,
	expr query.Expression,
	opts query.Options,
) ([]*apirun.Run, error) {

	qargs := []any{}
	wheres := []string{}

	switch expr := expr.(type) {
	case query.UnaryExpression:
		pred := expr.Predicate
		switch pred := pred.(type) {
		case apirun.UUIDPredicate:
			op := pred.Op
			switch op {
			case query.PredicateOperatorEqual:
				u := pred.Value.(string)
				wheres = append(wheres, fmt.Sprintf("r.uuid = $%d", len(qargs)+1))
				qargs = append(qargs, u)
			case query.PredicateOperatorIn:
				us := pred.Value.([]string)
				wheres = append(wheres, fmt.Sprintf("r.uuid = ANY($%d)", len(qargs)+1))
				qargs = append(qargs, us)
			}
		}
	case query.OrExpression:
		subexprs := expr.Expressions()
		ors := make([]string, 0, len(subexprs))
		for _, subexpr := range subexprs {
			switch subexpr := subexpr.(type) {
			case query.UnaryExpression:
				pred := subexpr.Predicate
				switch pred := pred.(type) {
				case apirun.UUIDPredicate:
					op := pred.Op
					switch op {
					case query.PredicateOperatorEqual:
						u := pred.Value.(string)
						ors = append(ors, fmt.Sprintf("r.uuid = $%d", len(qargs)+1))
						qargs = append(qargs, u)
					case query.PredicateOperatorIn:
						us := pred.Value.([]string)
						ors = append(ors, fmt.Sprintf("r.uuid = ANY($%d)", len(qargs)+1))
						qargs = append(qargs, us)
					}
				}
			}
		}
		wheres = append(wheres, "("+strings.Join(ors, ") OR (")+")")
	case query.AndExpression:
		subexprs := expr.Expressions()
		ands := make([]string, 0, len(subexprs))
		for _, subexpr := range subexprs {
			switch subexpr := subexpr.(type) {
			case query.UnaryExpression:
				pred := subexpr.Predicate
				switch pred := pred.(type) {
				case apirun.UUIDPredicate:
					op := pred.Op
					switch op {
					case query.PredicateOperatorEqual:
						u := pred.Value.(string)
						ands = append(ands, fmt.Sprintf("r.uuid = $%d", len(qargs)+1))
						qargs = append(qargs, u)
					}
				}
			}
		}
		wheres = append(wheres, "("+strings.Join(ands, ") AND (")+")")
	}

	var recs []runRecord
	fn := func(tx pgx.Tx) error {
		qs := `
SELECT
  r.id AS run_id
, r.uuid AS run_uuid
, t.uuid AS target_uuid
, t.generation AS target_generation
, r.root AS root_id
, r.parent AS parent_id
, rr.created_on AS requested_on
, r.scheduled_on
, rr.caller_identity AS caller_identity
, rr.caller_system_id AS caller_system_id
, rr.caller_domain_id AS caller_domain_id
, rr.in_vars AS in_vars
, rr.options AS options
, r.started_on
, r.completed_on
, r.failed_on
, r.paused_on
, r.resumed_on
, r.canceled_on
, r.finalized_on
FROM runs AS r
INNER JOIN run_requests AS rr
 ON r.id = rr.run
INNER JOIN object_generations AS t
 ON r.target = t.id
`
		if len(wheres) > 0 {
			qs += "WHERE " + strings.Join(wheres, " AND ")
		}
		qs += fmt.Sprintf("\nORDER BY r.uuid ASC LIMIT %d", opts.Limit())
		rows, err := tx.Query(ctx, qs, qargs...)
		if err != nil {
			return errors.Internal(
				"failed reading run records",
				errors.WithWrap(err),
			)
		}
		defer rows.Close()
		recs, err = pgx.CollectRows(rows, pgx.RowToStructByName[runRecord])
		if err != nil {
			return errors.Internal(
				"failed collecting run records",
				errors.WithWrap(err),
			)
		}

		return nil
	}
	if err := s.Exec(ctx, fn); err != nil {
		return nil, err
	}
	out := make([]*apirun.Run, 0, len(recs))
	for _, rec := range recs {
		caller := api.Caller{
			Identity: rec.CallerIdentity,
		}
		target := apirun.Target{
			UUID:       rec.TargetUUID,
			Generation: rec.TargetGeneration,
		}
		rr := apirun.Request{
			UUID:   rec.UUID,
			Target: target,
			Caller: caller,
			On:     time.Unix(0, rec.CreatedOn),
		}
		if rec.Options.Valid {
			rr.Options = rec.Options.String
		}
		if rec.InVars.Valid {
			rr.In = rec.InVars.String
		}
		r := &apirun.Run{}
		r.SetSystemInternalID(rec.ID)
		r.SetRequest(rr)
		if rec.StartedOn.Valid {
			r.SetStartedOn(time.Unix(0, rec.StartedOn.Int64))
		}
		if rec.CompletedOn.Valid {
			r.SetCompletedOn(time.Unix(0, rec.CompletedOn.Int64))
		}
		if rec.FailedOn.Valid {
			r.SetFailedOn(time.Unix(0, rec.FailedOn.Int64))
		}
		if rec.PausedOn.Valid {
			r.SetPausedOn(time.Unix(0, rec.PausedOn.Int64))
		}
		if rec.ResumedOn.Valid {
			r.SetResumedOn(time.Unix(0, rec.ResumedOn.Int64))
		}
		if rec.CanceledOn.Valid {
			r.SetCanceledOn(time.Unix(0, rec.CanceledOn.Int64))
		}
		out = append(out, r)
	}
	return out, nil
}
