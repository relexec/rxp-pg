package store

import (
	"context"

	"github.com/relexec/rxp/api"

	storerun "github.com/relexec/rxp-pg/internal/store/run"
)

// Write atomically writes the supplied RunEvents to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	runRec storerun.Record,
	events []api.RunEvent,
) error {
	return s.dbInsert(ctx, runRec, events)
}
