package store

import (
	"context"

	"github.com/relexec/rxp/api"

	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Write atomically writes the pre-validated Kind to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec storesystem.Record,
	kind api.Kind,
) error {
	return s.dbInsert(ctx, sysRec, kind)
}
