package store

import (
	"context"

	"github.com/relexec/rxp"
)

// Write atomically writes the supplied System to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sys rxp.System,
) error {
	return s.dbInsert(ctx, sys)
}
