package store

import (
	"context"

	"github.com/relexec/rxp/system"
)

// Write atomically writes the supplied System to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sys *system.System,
) error {
	return s.dbInsert(ctx, sys)
}
