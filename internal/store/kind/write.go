package store

import (
	"context"

	"github.com/relexec/rxp/api"
)

// Write atomically writes the pre-validated Kind to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec *api.System,
	kind api.Kind,
) error {
	return s.dbInsert(ctx, sysRec, kind)
}
