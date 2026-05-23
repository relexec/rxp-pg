package store

import (
	"context"

	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/errors"
)

// Write atomically writes the pre-validated Domain to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	dom *domain.Domain,
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
