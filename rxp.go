package rxppg

import (
	"context"

	"github.com/relexec/rxp"
	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/driver"
)

var WithHostSystemUUID = driver.WithHostSystemUUID
var WithHostSystemTag = driver.WithHostSystemTag
var WithLogger = driver.WithLogger
var WithMetrics = driver.WithMetrics

// New returns a new rxp Driver that uses PostgreSQL as backend persistence.
func New(
	ctx context.Context,
	cfg config.Config,
	opts ...driver.WithOption,
) (rxp.Driver, error) {
	return driver.New(ctx, cfg, opts...)
}
