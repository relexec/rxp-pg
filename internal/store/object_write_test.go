package store_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	testutil "github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/object"
	writeoption "github.com/relexec/rxp/object/write/option"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/application"
	"github.com/relexec/rxp/testing/fixtures/book"
	"github.com/relexec/rxp/testing/fixtures/platform"
	"github.com/relexec/rxp/testing/fixtures/service"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestObjectWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	err = testutil.EnsureKind(ctx, s, platform.Kind)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, platform.FirstMeta())
	require.Nil(t, err)

	// NOTE: Platform is NamescopeSystem which allows us to test the
	// system-qualified name constraints.
	plat1 := object.New(
		object.WithKindVersion(platform.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithName(testutil.RandomName()),
	)
	plat1Name := plat1.Name()
	platDuplicateName := object.New(
		object.WithKindVersion(platform.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithName(plat1Name),
	)

	domain := fixtures.Domain
	err = testutil.EnsureDomain(ctx, s, domain)
	require.Nil(t, err)

	ns := fixtures.Namespace
	err = testutil.EnsureNamespace(ctx, s, ns)
	require.Nil(t, err)

	err = testutil.EnsureKind(ctx, s, application.Kind)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, application.FirstMeta())
	require.Nil(t, err)

	// NOTE: Application is NamescopeDomain which allows us to test the
	// domain-qualified name constraints.
	appMissingDomain := object.New(
		object.WithKindVersion(application.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithName(testutil.RandomName()),
	)
	app1 := object.New(
		object.WithKindVersion(application.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithName(testutil.RandomName()),
	)
	app1Name := app1.Name()
	appDuplicateName := object.New(
		object.WithKindVersion(application.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithName(app1Name),
	)

	err = testutil.EnsureKind(ctx, s, service.Kind)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, service.FirstMeta())
	require.Nil(t, err)

	// NOTE: Service is NamescopeNamespace which allows us to test the
	// namespace-qualified name constraints.
	svc1 := object.New(
		object.WithKindVersion(service.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
		object.WithName(testutil.RandomName()),
	)
	svc1Name := svc1.Name()
	svcDuplicateName := object.New(
		object.WithKindVersion(service.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
		object.WithName(svc1Name),
	)

	ctxMissingIdent := context.TODO()

	svcMissingUUID := object.New(
		object.WithKindVersion(book.FirstKindVersion()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
		object.WithName(testutil.RandomName()),
	)
	svcMissingName := object.New(
		object.WithKindVersion(service.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
	)
	svcMissingNamespace := object.New(
		object.WithKindVersion(service.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithName(testutil.RandomName()),
		object.WithDomain(domain),
	)

	cases := []struct {
		name    string
		ctx     context.Context
		subject rxptypes.Object
		opts    []writeoption.Option
		exp     rxptypes.Object
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownObject,
			nil,
			nil,
			"missing identity",
		},
		{
			"missing uuid",
			ctx,
			svcMissingUUID,
			nil,
			nil,
			"invalid object: uuid required",
		},
		{
			"missing name",
			ctx,
			svcMissingName,
			nil,
			nil,
			"invalid object: name required",
		},
		{
			"domain required",
			ctx,
			appMissingDomain,
			nil,
			nil,
			"invalid object: domain required",
		},
		{
			"namespace required",
			ctx,
			svcMissingNamespace,
			nil,
			nil,
			"invalid object: namespace required",
		},
		{
			"unknown kind version",
			ctx,
			fixtures.UnknownObject,
			nil,
			nil,
			"unknown kind version",
		},
		{
			"happy path system-scoped object",
			ctx,
			plat1,
			nil,
			plat1,
			"",
		},
		{
			"system-qualified name collision",
			ctx,
			platDuplicateName,
			nil,
			nil,
			"conflict: \"platform.testing.rxp\" already exists with name",
		},
		{
			"happy path domain-scoped object",
			ctx,
			app1,
			nil,
			app1,
			"",
		},
		{
			"domain-qualified name collision",
			ctx,
			appDuplicateName,
			nil,
			nil,
			"conflict: \"application.testing.rxp\" already exists with name",
		},
		{
			"happy path namespace-scoped object",
			ctx,
			svc1,
			nil,
			svc1,
			"",
		},
		{
			"namespace-qualified name collision",
			ctx,
			svcDuplicateName,
			nil,
			nil,
			"conflict: \"service.testing.rxp\" already exists with name",
		},
		// Attempting to write the exact same object without specifying an
		// expected generation should result in a precondition failed.
		{
			"duplicate UUID",
			ctx,
			svc1,
			nil,
			svc1,
			"not to exist",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := s.ObjectWrite(c.ctx, c.subject, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
