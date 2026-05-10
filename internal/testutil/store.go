package testutil

import (
	"context"
	"sync"

	domainselector "github.com/relexec/rxp/domain/read/selector"
	"github.com/relexec/rxp/errors"
	metaselector "github.com/relexec/rxp/meta/read/selector"
	namespaceselector "github.com/relexec/rxp/namespace/read/selector"
	objectselector "github.com/relexec/rxp/object/read/selector"
	"github.com/relexec/rxp/testing/fixtures"
	rxptypes "github.com/relexec/rxp/types"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/store"
)

const (
	DSN = "host=localhost port=5432 user=postgres password=postgres dbname=rxptest"
)

var (
	once      sync.Once
	testStore *store.Store
)

// Store returns a Store that is connected to the local testing database.
func Store(ctx context.Context) (*store.Store, error) {
	var err error
	once.Do(func() {
		metrics, err := Metrics(ctx)
		if err != nil {
			return
		}
		cfg := config.New(config.WithConnect(DSN))
		testStore, err = store.New(
			ctx,
			store.WithHostSystemUUID(fixtures.SystemUUID),
			store.WithHostSystemName(fixtures.SystemName),
			store.WithMetrics(metrics),
			store.WithConfig(cfg),
		)
	})
	return testStore, err
}

// EnsureDomain ensures that the supplied Domain exists in the database.
func EnsureDomain(
	ctx context.Context,
	s *store.Store,
	d rxptypes.Domain,
) error {
	_, err := s.DomainRead(
		ctx,
		domainselector.New(
			domainselector.WithName(d.Name()),
		),
	)
	if err != nil {
		if err != errors.ErrNotFound {
			return err
		}
		return s.DomainWrite(ctx, d)
	}
	return nil
}

// EnsureNamespace ensures that the supplied Namespace exists in the database.
func EnsureNamespace(
	ctx context.Context,
	s *store.Store,
	ns rxptypes.Namespace,
) error {
	_, err := s.NamespaceRead(
		ctx,
		namespaceselector.New(
			namespaceselector.WithDomain(ns.Domain()),
			namespaceselector.WithName(ns.Name()),
		),
	)
	if err != nil {
		if err != errors.ErrNotFound {
			return err
		}
		return s.NamespaceWrite(ctx, ns)
	}
	return nil
}

// EnsureMeta ensures that the supplied Meta exists in the database.
func EnsureMeta(
	ctx context.Context,
	s *store.Store,
	m rxptypes.Meta,
) error {
	_, err := s.MetaRead(
		ctx,
		metaselector.New(
			metaselector.WithKindVersion(m.KindVersion()),
		),
	)
	if err != nil {
		if err != errors.ErrNotFound {
			return err
		}
		return s.MetaWrite(ctx, m)
	}
	return nil
}

// EnsureObject ensures that the supplied Object exists in the database.
func EnsureObject(
	ctx context.Context,
	s *store.Store,
	o rxptypes.Object,
) error {
	_, err := s.ObjectRead(
		ctx,
		objectselector.New(
			objectselector.WithKindVersion(o.KindVersion()),
			objectselector.WithUUID(o.UUID()),
		),
	)
	if err != nil {
		if err != errors.ErrNotFound {
			return err
		}
		return s.ObjectWrite(ctx, o)
	}
	return nil
}
