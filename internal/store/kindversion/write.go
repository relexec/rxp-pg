package store

import (
	"context"

	"github.com/relexec/rxp/api"
	apikind "github.com/relexec/rxp/api/kind"
	apisystem "github.com/relexec/rxp/api/system"
)

// Write atomically writes the pre-validated KindVersion to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec *apisystem.System,
	kindRec *apikind.Kind,
	kv api.KindVersion,
) error {
	return s.dbInsert(ctx, sysRec, kindRec, kv)
}
