package testutil

import (
	"context"
	"sync"

	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind"
	"github.com/relexec/rxp/meta"
	"github.com/relexec/rxp/namespace"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/testing/fixtures"

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

// KindCreateIfNotExists ensures that the supplied Kind exists in the
// database.
func MetaCreateIfNotExists(
	ctx context.Context,
	d *driver.Driver,
	m *meta.Meta,
) error {
	_, err := d.MetaRead(
		ctx,
		meta.ByKindVersion(m.KindVersion()),
	)
	if err != nil {
		if err != errors.ErrNotFound {
			return err
		}
		return d.MetaWrite(ctx, m)
	}
	return nil
}

// KindCreateIfNotExists ensures that the supplied Kind exists in the
// database.
func KindCreateIfNotExists(
	ctx context.Context,
	d *driver.Driver,
	k *kind.Kind,
) error {
	_, err := d.KindRead(
		ctx,
		kind.ByName(k.Name()),
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
	dom *domain.Domain,
) error {
	_, err := d.DomainRead(
		ctx,
		domain.Select(domain.ByName(dom.Name())),
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
	ns *namespace.Namespace,
) error {
	_, err := d.NamespaceRead(
		ctx,
		namespace.ByName(
			ns.Domain(),
			ns.Name(),
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

// ObjectCreateIfNotExists ensures that the supplied Object exists in the
// database.
func ObjectCreateIfNotExists(
	ctx context.Context,
	d *driver.Driver,
	o *object.Object,
) error {
	selopts := []object.SelectOption{}
	if o.UUID() != "" {
		selopts = append(selopts, object.ByUUID(o.UUID()))
	} else if o.Name() != "" {
		selopts = append(selopts, object.ByName(o.Name()))
	}
	if o.Namespace() != nil {
		selopts = append(selopts, object.ByNamespace(o.Namespace()))
	} else if o.Domain() != nil {
		selopts = append(selopts, object.ByDomain(o.Domain()))
	}
	_, err := d.ObjectRead(ctx, o.KindVersion(), object.Select(selopts...))
	if err != nil {
		if err != errors.ErrNotFound {
			return err
		}
		return d.ObjectWrite(ctx, o)
	}
	return nil
}
