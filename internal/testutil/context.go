package testutil

import (
	"context"

	rxpcontext "github.com/relexec/rxp/context"
)

const (
	UserIdentity  = "user"
	AdminIdentity = "admin"
)

// Context returns a properly constructed context for use in testing.
func Context(identity string) context.Context {
	ctx := context.TODO()
	ctx = rxpcontext.SetIdentity(ctx, identity)
	return ctx
}
