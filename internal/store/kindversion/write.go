package store

import (
	"context"

	rxpkind "github.com/relexec/rxp/api/kind"
	rxpkindversion "github.com/relexec/rxp/api/kindversion"
	rxpsystem "github.com/relexec/rxp/api/system"
)

// Write atomically writes the pre-validated KindVersion to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec *rxpsystem.System,
	kindRec *rxpkind.Kind,
	kv rxpkindversion.KindVersion,
) error {
	return s.dbInsert(ctx, sysRec, kindRec, kv)
}
