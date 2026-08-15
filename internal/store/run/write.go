package store

import (
	"context"

	"github.com/relexec/rxp/api"

	storeobject "github.com/relexec/rxp-pg/internal/store/object"
)

// Write atomically writes the supplied Run to persistent storage. On
// successful write, the newly created Run is returned.
func (s *Store) Write(
	ctx context.Context,
	targetRec storeobject.Record,
	callerSysRec *api.System,
	callerDomRec *api.Domain,
	rootRec *Record,
	parentRec *Record,
	run api.Run,
) (*api.Run, error) {
	return s.dbInsert(
		ctx, targetRec,
		callerSysRec, callerDomRec,
		rootRec, parentRec, run,
	)
}
