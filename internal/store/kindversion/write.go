package store

import (
	"context"

	apikind "github.com/relexec/rxp/api/kind"
	apikindversion "github.com/relexec/rxp/api/kindversion"
	apisystem "github.com/relexec/rxp/api/system"
)

// Write atomically writes the pre-validated KindVersion to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec *apisystem.System,
	kindRec *apikind.Kind,
	kv apikindversion.KindVersion,
) error {
	return s.dbInsert(ctx, sysRec, kindRec, kv)
}
