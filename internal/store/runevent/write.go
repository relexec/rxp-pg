package store

import (
	"context"

	apirun "github.com/relexec/rxp/api/run"
)

// Write atomically writes the supplied RunEvents to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	run apirun.Run,
	events []apirun.Event,
) error {
	return s.dbInsert(ctx, run, events)
}
