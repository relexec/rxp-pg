package store

import (
	"context"

	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
	rxptypes "github.com/relexec/rxp/types"
)

// readValidate performs some basic verification of the read request.
func (s *Store) readValidate(
	ctx context.Context,
	kv rxptypes.KindVersion,
) error {
	ident := rxpcontext.Identity(ctx)
	if ident == "" {
		return errors.ErrMissingIdentity
	}
	return kv.Validate()
}
