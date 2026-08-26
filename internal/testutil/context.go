package testutil

import (
	"context"

	apicore "github.com/relexec/rxp/api/core"
)

const (
	UserIdentity  = "user"
	AdminIdentity = "admin"
)

// Context returns a properly constructed context for use in testing.
func Context(identity string) context.Context {
	ctx := context.TODO()
	caller := apicore.Caller{Identity: identity}
	ctx = apicore.CallerToContext(ctx, caller)
	return ctx
}
