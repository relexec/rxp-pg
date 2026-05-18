package store

import (
	"context"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/types"
)

// Write atomically writes the pre-validated Namespace to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	ns types.Namespace,
) error {
	dom := ns.Domain()
	domRec, err := s.domainStore.ReadByUUID(ctx, dom.UUID())
	if err != nil {
		return errors.Internal(
			"failed reading domain record",
			errors.WithWrap(err),
		)
	}
	return s.dbInsert(ctx, domRec, ns)
}
