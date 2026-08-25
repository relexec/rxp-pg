package store

import (
	"context"

	apikind "github.com/relexec/rxp/api/kind"
	apisystem "github.com/relexec/rxp/api/system"
)

// Write atomically writes the pre-validated Kind to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec *apisystem.System,
	kind apikind.Kind,
) error {
	return s.dbInsert(ctx, sysRec, kind)
}
