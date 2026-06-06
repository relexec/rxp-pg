package rxppg

import (
	"context"

	"github.com/relexec/rxp"
	"github.com/relexec/rxp-pg/internal/driver"
)

var WithHostSystemUUID = driver.WithHostSystemUUID
var WithHostSystemTag = driver.WithHostSystemTag
var WithLogger = driver.WithLogger
var WithConfig = driver.WithConfig
var WithMetrics = driver.WithMetrics

// New returns a new rxp Driver that uses PostgreSQL as backend persistence.
func New(
	ctx context.Context,
	opts ...driver.WithOption,
) (rxp.Driver, error) {
	return driver.New(ctx, opts...)
}
