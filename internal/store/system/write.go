package store

import (
	"context"

	apisystem "github.com/relexec/rxp/api/system"
)

// Write atomically writes the supplied System to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sys apisystem.System,
) error {
	return s.dbInsert(ctx, sys)
}
