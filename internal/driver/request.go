package driver

import (
	"context"

	apicore "github.com/relexec/rxp/api/core"
	apierrors "github.com/relexec/rxp/api/errors"
)

// requestValidate performs some basic verification of the request.
func (d *Driver) requestValidate(
	ctx context.Context,
) error {
	caller := apicore.CallerFromContext(ctx)
	if caller == nil || caller.Identity == "" {
		return apierrors.ErrMissingIdentity
	}
	return nil
}
