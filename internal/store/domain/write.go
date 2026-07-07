package store

import (
	"context"

	storesystem "github.com/relexec/rxp-pg/internal/store/system"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/errors"
)

// Write atomically writes the pre-validated Domain to persistent storage.
func (s *Store) Write(
	ctx context.Context,
	sysRec storesystem.Record,
	dom domain.Domain,
) error {
	err := s.dbInsert(ctx, sysRec, dom)
	if err != nil {
		return err
	}
	if dom.Parent() != nil {
		// Simply wipe out the cache entries for all domains in the domain
		// tree, since the nested set left/right values will have changed for
		// all nodes in the domain tree. This is easier and faster than trying
		// to update each cache entry and we rely on the
		// write-to-cache-on-read-miss behaviour to keep cache entries fresh.
		if err = s.cacheEvict(ctx, dom); err != nil {
			return errors.Internal(
				"failed evicting domain records from cache",
				errors.WithWrap(err),
			)
		}
	}
	return nil
}
