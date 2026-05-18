package testutil

import (
	"context"
	"sync"

	"github.com/relexec/rxp/testing/fixtures"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/driver"
)

var (
	driverOnce sync.Once
	testDriver *driver.Driver
)

// Driver returns a Driver that uses a local test database for its store.
func Driver(ctx context.Context) (*driver.Driver, error) {
	var err error
	driverOnce.Do(func() {
		metrics, err := Metrics(ctx)
		if err != nil {
			return
		}
		cfg := config.New(config.WithConnect(DSN))
		testDriver, err = driver.New(
			ctx,
			driver.WithHostSystemUUID(fixtures.SystemUUID),
			driver.WithHostSystemName(fixtures.SystemName),
			driver.WithMetrics(metrics),
			driver.WithConfig(cfg),
		)
	})
	return testDriver, err
}
