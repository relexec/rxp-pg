package store

import (
	"context"

	"github.com/relexec/rxp/api"
	apisystem "github.com/relexec/rxp/api/system"
)

// Write atomically writes the pre-validated Kind to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec *apisystem.System,
	kind api.Kind,
) error {
	return s.dbInsert(ctx, sysRec, kind)
}
