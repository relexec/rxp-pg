package store

import (
	"context"

	"github.com/relexec/rxp/api"
)

// Record decorates an Run with internal DB information.
type Record struct {
	// RowID is the internal database SERIAL for the runs record.
	RowID int64
	// Run is the publicly-exposed Run object.
	Run *api.Run
}

// ReadByRowID returns a Record for the Run with the supplied internal DB row
// ID.
func (s *Store) ReadByRowID(
	ctx context.Context,
	rowID int64,
) (*Record, error) {
	return s.dbReadByRowID(ctx, rowID)
}

// ReadByUUID returns a Record for the Run with the supplied UUID.
func (s *Store) ReadByUUID(
	ctx context.Context,
	uuid string,
) (*Record, error) {
	return s.dbReadByUUID(ctx, uuid)
}
