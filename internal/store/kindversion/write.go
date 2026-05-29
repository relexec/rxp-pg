package store

import (
	"context"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind/kindversion"
)

// Write atomically writes the pre-validated KindVersion to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	kv *kindversion.KindVersion,
) error {
	sys := kv.System()
	sysRec, err := s.systemStore.ReadByUUID(ctx, sys.UUID())
	if err != nil {
		return errors.Internal(
			"failed reading system record",
			errors.WithWrap(err),
		)
	}
	k := kv.Kind()
	kindRec, err := s.kindStore.ReadByName(ctx, sys, k.Name())
	if err != nil {
		return errors.Internal(
			"failed reading kind record",
			errors.WithWrap(err),
		)
	}
	return s.dbInsert(ctx, sysRec, kindRec, kv)
}
