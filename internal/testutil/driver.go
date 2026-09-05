package testutil

import (
	"context"
	"sync"

	"github.com/relexec/rxp-testing/fixtures"
	rxpdomain "github.com/relexec/rxp/api/domain"
	apierrors "github.com/relexec/rxp/api/errors"
	rxpkind "github.com/relexec/rxp/api/kind"
	rxpkindversion "github.com/relexec/rxp/api/kindversion"
	rxpobject "github.com/relexec/rxp/api/object"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/driver"
)

const (
	DSN = "host=localhost port=5432 user=postgres password=postgres dbname=rxptest"
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
		d, err := driver.New(
			ctx, cfg,
			driver.WithHostSystemUUID(fixtures.SystemUUID),
			driver.WithHostSystemTag(fixtures.SystemTag),
			driver.WithMetrics(metrics),
		)
		if err == nil {
			testDriver = d
		}
	})
	return testDriver, err
}

// KindVersionCreateIfNotExists ensures that the supplied KindVersion exists in
// the database.
func KindVersionCreateIfNotExists(
	ctx context.Context,
	d *driver.Driver,
	kv *rxpkindversion.KindVersion,
) error {
	_, err := d.KindVersionRead(
		ctx,
		rxpkindversion.Select(
			rxpkindversion.ByName(kv.Name()),
		),
	)
	if err != nil {
		if err != apierrors.ErrNotFound {
			return err
		}
		return d.KindVersionWrite(ctx, *kv)
	}
	return nil
}

// KindCreateIfNotExists ensures that the supplied Kind exists in the
// database.
func KindCreateIfNotExists(
	ctx context.Context,
	d *driver.Driver,
	k rxpkind.Kind,
) error {
	_, err := d.KindRead(
		ctx,
		rxpkind.Select(rxpkind.ByName(k.Name)),
	)
	if err != nil {
		if err != apierrors.ErrNotFound {
			return err
		}
		return d.KindWrite(ctx, k)
	}
	return nil
}

// DomainCreateIfNotExists ensures that the supplied Domain exists in the
// database.
func DomainCreateIfNotExists(
	ctx context.Context,
	d *driver.Driver,
	dom rxpdomain.Domain,
) error {
	_, err := d.DomainRead(
		ctx,
		rxpdomain.Select(rxpdomain.ByName(dom.Name)),
	)
	if err != nil {
		if err != apierrors.ErrNotFound {
			return err
		}
		return d.DomainWrite(ctx, dom)
	}
	return nil
}

// ObjectCreateIfNotExists ensures that the supplied Object exists in the
// database.
func ObjectCreateIfNotExists(
	ctx context.Context,
	d *driver.Driver,
	o *rxpobject.Object,
) error {
	selopts := []rxpobject.SelectOption{}
	if o.UUID != "" {
		selopts = append(selopts, rxpobject.ByUUID(o.UUID))
	} else if o.Name != "" {
		selopts = append(selopts, rxpobject.ByName(o.Name))
	}
	if o.Domain != nil {
		selopts = append(selopts, rxpobject.ByDomain(o.Domain))
	}
	_, err := d.ObjectRead(ctx, o.KindVersionName, rxpobject.Select(selopts...))
	if err != nil {
		if err != apierrors.ErrNotFound {
			return err
		}
		_, err := d.ObjectWrite(ctx, *o)
		return err
	}
	return nil
}
