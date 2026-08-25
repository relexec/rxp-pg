package testutil

import (
	"context"
	"sync"

	"github.com/relexec/rxp-testing/fixtures"
	"github.com/relexec/rxp/api"
	apidomain "github.com/relexec/rxp/api/domain"
	apikind "github.com/relexec/rxp/api/kind"
	apiobject "github.com/relexec/rxp/api/object"
	apirun "github.com/relexec/rxp/api/run"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind/kindversion"

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
	kv *api.KindVersion,
) error {
	_, err := d.KindVersionRead(
		ctx,
		kindversion.Select(
			kindversion.ByName(kv.Name()),
		),
	)
	if err != nil {
		if err != errors.ErrNotFound {
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
	k apikind.Kind,
) error {
	_, err := d.KindRead(
		ctx,
		apikind.Select(apikind.ByName(k.Name)),
	)
	if err != nil {
		if err != errors.ErrNotFound {
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
	dom apidomain.Domain,
) error {
	_, err := d.DomainRead(
		ctx,
		apidomain.Select(apidomain.ByName(dom.Name)),
	)
	if err != nil {
		if err != errors.ErrNotFound {
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
	o *apiobject.Object,
) error {
	selopts := []apiobject.SelectOption{}
	if o.UUID != "" {
		selopts = append(selopts, apiobject.ByUUID(o.UUID))
	} else if o.Name != "" {
		selopts = append(selopts, apiobject.ByName(o.Name))
	}
	if o.Domain != nil {
		selopts = append(selopts, apiobject.ByDomain(o.Domain))
	}
	_, err := d.ObjectRead(ctx, o.KindVersionName, apiobject.Select(selopts...))
	if err != nil {
		if err != errors.ErrNotFound {
			return err
		}
		_, err := d.ObjectWrite(ctx, *o)
		return err
	}
	return nil
}

// RunCreateIfNotExists ensures that the supplied Run exists in the
// database.
func RunCreateIfNotExists(
	ctx context.Context,
	d *driver.Driver,
	r *apirun.Run,
) error {
	selopts := []apirun.SelectOption{apirun.ByUUID(r.UUID())}
	_, err := d.RunRead(
		ctx, r.Request().Target, apirun.Select(selopts...),
	)
	if err != nil {
		if err != errors.ErrNotFound {
			return err
		}
		_, err := d.RunWrite(ctx, *r)
		return err
	}
	return nil
}
