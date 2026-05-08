package store

import (
	"context"

	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
)

// requestValidate performs some basic verification of the request.
func (s *Store) requestValidate(
	ctx context.Context,
) error {
	ident := rxpcontext.Identity(ctx)
	if ident == "" {
		return errors.ErrMissingIdentity
	}
	return nil
}
