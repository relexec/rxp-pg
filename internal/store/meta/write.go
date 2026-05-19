package store

import (
	"context"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/types"
)

// Write atomically writes the pre-validated Meta to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	m types.Meta,
) error {
	sys := m.System()
	sysRec, err := s.systemStore.ReadByUUID(ctx, sys.UUID())
	if err != nil {
		return errors.Internal(
			"failed reading system record",
			errors.WithWrap(err),
		)
	}
	kv := m.KindVersion()
	k := kv.Kind()
	kindRec, err := s.kindStore.ReadByName(ctx, sys, k)
	if err != nil {
		return errors.Internal(
			"failed reading kind record",
			errors.WithWrap(err),
		)
	}
	return s.dbInsert(ctx, sysRec, kindRec, m)
}
