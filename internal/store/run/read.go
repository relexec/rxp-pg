package store

import (
	"context"

	rxprun "github.com/relexec/rxp/api/run"
)

// ReadByRowID returns a Record for the Run with the supplied internal DB row
// ID.
func (s *Store) ReadByRowID(
	ctx context.Context,
	rowID int64,
) (*rxprun.Run, error) {
	return s.dbReadByRowID(ctx, rowID)
}

// ReadByUUID returns a Record for the Run with the supplied UUID.
func (s *Store) ReadByUUID(
	ctx context.Context,
	uuid string,
) (*rxprun.Run, error) {
	return s.dbReadByUUID(ctx, uuid)
}
