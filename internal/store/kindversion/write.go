package store

import (
	"context"

	"github.com/relexec/rxp"
)

// Write atomically writes the pre-validated KindVersion to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec *rxp.System,
	kindRec *rxp.Kind,
	kv rxp.KindVersion,
) error {
	return s.dbInsert(ctx, sysRec, kindRec, kv)
}
