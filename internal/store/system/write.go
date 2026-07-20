package store

import (
	"context"

	"github.com/relexec/rxp/api"
)

// Write atomically writes the supplied System to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sys api.System,
) error {
	return s.dbInsert(ctx, sys)
}
