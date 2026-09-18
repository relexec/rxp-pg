package driver

import (
	"context"

	"github.com/relexec/rxp"
)

// requestValidate performs some basic verification of the request.
func (d *Driver) requestValidate(
	ctx context.Context,
) error {
	caller := rxp.CallerFromContext(ctx)
	if caller == nil || caller.Identity == "" {
		return rxp.ErrMissingIdentity
	}
	return nil
}
