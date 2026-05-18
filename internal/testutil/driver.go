package testutil

import (
	"context"
	"sync"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/name"
	"github.com/relexec/rxp/read/selector"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/types"

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

// DomainCreateIfNotExists ensures that the supplied Domain exists in the
// database.
func DomainCreateIfNotExists(
	ctx context.Context,
	d *driver.Driver,
	dom types.Domain,
) error {
	_, err := d.DomainRead(
		ctx,
		selector.New(
			selector.WithName(
				name.New(string(dom.Name())),
			),
		),
	)
	if err != nil {
		if err != errors.ErrNotFound {
			return err
		}
		return d.DomainWrite(ctx, dom)
	}
	return nil
}

// NamespaceCreateIfNotExists ensures that the supplied Namespace exists in the
// database.
func NamespaceCreateIfNotExists(
	ctx context.Context,
	d *driver.Driver,
	ns types.Namespace,
) error {
	_, err := d.NamespaceRead(
		ctx,
		selector.New(
			selector.WithName(
				name.New(
					string(ns.Name()),
					name.WithDomain(ns.Domain()),
				),
			),
		),
	)
	if err != nil {
		if err != errors.ErrNotFound {
			return err
		}
		return d.NamespaceWrite(ctx, ns)
	}
	return nil
}
