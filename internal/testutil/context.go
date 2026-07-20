package testutil

import (
	"context"

	"github.com/relexec/rxp/api"
)

const (
	UserIdentity  = "user"
	AdminIdentity = "admin"
)

// Context returns a properly constructed context for use in testing.
func Context(identity string) context.Context {
	ctx := context.TODO()
	caller := api.Caller{Identity: identity}
	ctx = api.CallerToContext(ctx, caller)
	return ctx
}
