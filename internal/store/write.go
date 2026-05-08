package store

import (
	"context"

	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
)

// writeValidate performs some basic verification of the write request.
func (s *Store) writeValidate(
	ctx context.Context,
) error {
	ident := rxpcontext.Identity(ctx)
	if ident == "" {
		return errors.ErrMissingIdentity
	}
	return nil
}
