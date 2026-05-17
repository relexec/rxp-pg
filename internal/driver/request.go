package driver

import (
	"context"

	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
)

// requestValidate performs some basic verification of the request.
func (d *Driver) requestValidate(
	ctx context.Context,
) error {
	ident := rxpcontext.Identity(ctx)
	if ident == "" {
		return errors.ErrMissingIdentity
	}
	return nil
}
