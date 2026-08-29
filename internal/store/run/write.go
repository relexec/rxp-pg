package store

import (
	"context"

	rxpdomain "github.com/relexec/rxp/api/domain"
	rxprun "github.com/relexec/rxp/api/run"
	rxpsystem "github.com/relexec/rxp/api/system"

	storeobject "github.com/relexec/rxp-pg/internal/store/object"
)

// Write atomically writes the supplied Run to persistent storage. On
// successful write, the newly created Run is returned.
func (s *Store) Write(
	ctx context.Context,
	targetRec storeobject.Record,
	callerSysRec *rxpsystem.System,
	callerDomRec *rxpdomain.Domain,
	root *rxprun.Identifiers,
	parent *rxprun.Identifiers,
	run rxprun.Run,
) (*rxprun.Run, error) {
	return s.dbInsert(
		ctx, targetRec,
		callerSysRec, callerDomRec,
		root, parent, run,
	)
}
