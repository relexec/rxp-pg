package driver

import (
	"context"

	apicore "github.com/relexec/rxp/api/core"
	"github.com/relexec/rxp/errors"
)

// requestValidate performs some basic verification of the request.
func (d *Driver) requestValidate(
	ctx context.Context,
) error {
	caller := apicore.CallerFromContext(ctx)
	if caller == nil || caller.Identity == "" {
		return errors.ErrMissingIdentity
	}
	return nil
}
