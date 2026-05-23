package store

import (
	"context"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind"
)

// Write atomically writes the pre-validated Kind to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	kind *kind.Kind,
) error {
	sys := kind.System()
	sysRec, err := s.systemStore.ReadByUUID(ctx, sys.UUID())
	if err != nil {
		return errors.Internal(
			"failed reading system record",
			errors.WithWrap(err),
		)
	}
	return s.dbInsert(ctx, sysRec, kind)
}
