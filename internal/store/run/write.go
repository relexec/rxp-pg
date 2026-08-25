package store

import (
	"context"

	apidomain "github.com/relexec/rxp/api/domain"
	apirun "github.com/relexec/rxp/api/run"
	apisystem "github.com/relexec/rxp/api/system"

	storeobject "github.com/relexec/rxp-pg/internal/store/object"
)

// Write atomically writes the supplied Run to persistent storage. On
// successful write, the newly created Run is returned.
func (s *Store) Write(
	ctx context.Context,
	targetRec storeobject.Record,
	callerSysRec *apisystem.System,
	callerDomRec *apidomain.Domain,
	root *apirun.Identifiers,
	parent *apirun.Identifiers,
	run apirun.Run,
) (*apirun.Run, error) {
	return s.dbInsert(
		ctx, targetRec,
		callerSysRec, callerDomRec,
		root, parent, run,
	)
}
