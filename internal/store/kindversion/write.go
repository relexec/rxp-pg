package store

import (
	"context"

	"github.com/relexec/rxp/api"
)

// Write atomically writes the pre-validated KindVersion to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec *api.System,
	kindRec *api.Kind,
	kv api.KindVersion,
) error {
	return s.dbInsert(ctx, sysRec, kindRec, kv)
}
