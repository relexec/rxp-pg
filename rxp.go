package rxppg

import (
	"context"

	"github.com/relexec/rxp"

	"github.com/relexec/rxp-pg/internal/driver"
)

// New returns a new rxp Driver that uses PostgreSQL as backend persistence.
func New(ctx context.Context) (rxp.Driver, error) {
	return driver.New(ctx)
}
