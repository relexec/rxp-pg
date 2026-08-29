package store

import (
	"context"

	rxpkind "github.com/relexec/rxp/api/kind"
	rxpsystem "github.com/relexec/rxp/api/system"
)

// Write atomically writes the pre-validated Kind to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec *rxpsystem.System,
	kind rxpkind.Kind,
) error {
	return s.dbInsert(ctx, sysRec, kind)
}
