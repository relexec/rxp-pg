package store

import (
	"context"

	"github.com/relexec/rxp/kind/kindversion"

	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Write atomically writes the pre-validated KindVersion to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kv kindversion.KindVersion,
) error {
	return s.dbInsert(ctx, sysRec, kindRec, kv)
}
