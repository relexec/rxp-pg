package store

import (
	"github.com/relexec/rxp/errors"
)

var (
	ErrPoolNotInitialized    = errors.Internal("connection pool not initialized")
	ErrTypeMapNotInitialized = errors.Internal("typemap not initialized")
)
