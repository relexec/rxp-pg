package store

import (
	"context"

	"github.com/relexec/rxp/api"
	apisystem "github.com/relexec/rxp/api/system"
)

// Write atomically writes the pre-validated KindVersion to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec *apisystem.System,
	kindRec *api.Kind,
	kv api.KindVersion,
) error {
	return s.dbInsert(ctx, sysRec, kindRec, kv)
}
