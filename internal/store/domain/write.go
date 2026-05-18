package store

import (
	"context"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/types"
)

// Write atomically writes the pre-validate Domain to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	dom types.Domain,
) error {
	sys := dom.System()
	sysRec, err := s.systemStore.ReadByUUID(ctx, sys.UUID())
	if err != nil {
		return errors.Internal(
			"failed reading system record",
			errors.WithWrap(err),
		)
	}
	return s.dbInsert(ctx, sysRec, dom)
}
