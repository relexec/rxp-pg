package store

import (
	"context"

	rxprun "github.com/relexec/rxp/api/run"
)

// Write atomically writes the supplied RunEvents to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	run rxprun.Run,
	events []rxprun.Event,
) error {
	return s.dbInsert(ctx, run, events)
}
