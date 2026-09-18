package testutil

import (
	"context"

	rxpcore "github.com/relexec/rxp/core"
)

const (
	UserIdentity  = "user"
	AdminIdentity = "admin"
)

// Context returns a properly constructed context for use in testing.
func Context(identity string) context.Context {
	ctx := context.TODO()
	caller := rxpcore.Caller{Identity: identity}
	ctx = rxpcore.CallerToContext(ctx, caller)
	return ctx
}
