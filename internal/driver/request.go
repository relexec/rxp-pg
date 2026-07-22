package driver

import (
	"context"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
)

// requestValidate performs some basic verification of the request.
func (d *Driver) requestValidate(
	ctx context.Context,
) error {
	caller := api.CallerFromContext(ctx)
	if caller == nil || caller.Identity == "" {
		return errors.ErrMissingIdentity
	}
	return nil
}
