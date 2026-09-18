package store

import (
	"context"

	"github.com/relexec/rxp"
)

// Write atomically writes the pre-validated Kind to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec *rxp.System,
	kind rxp.Kind,
) error {
	return s.dbInsert(ctx, sysRec, kind)
}
