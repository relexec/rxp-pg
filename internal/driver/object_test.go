package driver_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	testutil "github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/list/expression"
	"github.com/relexec/rxp/list/option"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/application"
	"github.com/relexec/rxp/testing/fixtures/platform"
	"github.com/relexec/rxp/testing/fixtures/service"
	"github.com/relexec/rxp/types"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestObjectRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, platform.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, platform.FirstMeta())
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, application.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, application.FirstMeta())
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, service.FirstMeta())
	require.Nil(t, err)

	dom := fixtures.Domain
	err = testutil.DomainCreateIfNotExists(ctx, rxp, dom)
	require.Nil(t, err)

	ns := fixtures.Namespace
	err = testutil.NamespaceCreateIfNotExists(ctx, rxp, ns)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	app1 := object.New(
		object.WithKindVersion(application.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(dom),
		object.WithName(testutil.RandomName()),
	)

	err = testutil.ObjectCreateIfNotExists(ctx, rxp, app1)
	require.Nil(t, err, err)

	svc1 := object.New(
		object.WithKindVersion(service.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithNamespace(ns),
		object.WithName(testutil.RandomName()),
	)

	err = testutil.ObjectCreateIfNotExists(ctx, rxp, svc1)
	require.Nil(t, err, err)

	cases := []struct {
		name   string
		ctx    context.Context
		kv     api.KindVersion
		sel    object.Selector
		exp    *object.Object
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			service.FirstKindVersion(),
			object.Select(object.ByUUID(svc1.UUID())),
			nil,
			"missing identity",
		},
		{
			"unknown kind version",
			ctx,
			fixtures.UnknownKindVersion,
			object.Select(object.ByUUID(svc1.UUID())),
			nil,
			"unknown kind version",
		},
		{
			"invalid kind version",
			ctx,
			fixtures.InvalidKindVersion,
			object.Select(object.ByUUID(svc1.UUID())),
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"either uuid or name required in selector",
			ctx,
			service.FirstKindVersion(),
			object.Select(),
			nil,
			"invalid selector: uuid or name required",
		},
		{
			"missing domain with uuid",
			ctx,
			application.FirstKindVersion(),
			object.Select(object.ByUUID(app1.UUID())),
			nil,
			"invalid selector: domain required",
		},
		{
			"missing domain with name",
			ctx,
			application.FirstKindVersion(),
			object.Select(object.ByName(testutil.RandomName())),
			nil,
			"invalid selector: domain required",
		},
		{
			"missing namespace with uuid",
			ctx,
			service.FirstKindVersion(),
			object.Select(object.ByUUID(svc1.UUID())),
			nil,
			"invalid selector: namespace required",
		},
		{
			"missing namespace with name",
			ctx,
			service.FirstKindVersion(),
			object.Select(object.ByName(testutil.RandomName())),
			nil,
			"invalid selector: namespace required",
		},
		{
			"mismatched kind",
			ctx,
			application.FirstKindVersion(),
			object.Select(object.ByDomain(dom), object.ByUUID(svc1.UUID())),
			nil,
			"not found",
		},
		{
			"unknown generation",
			ctx,
			application.FirstKindVersion(),
			object.Select(
				object.ByDomain(dom),
				object.ByUUID(app1.UUID()),
				object.ByGeneration(42),
			),
			nil,
			"not found",
		},
		{
			"happy path by domain and uuid",
			ctx,
			application.FirstKindVersion(),
			object.Select(object.ByDomain(dom), object.ByUUID(app1.UUID())),
			app1,
			"",
		},
		{
			"happy path by domain-qualified name",
			ctx,
			application.FirstKindVersion(),
			object.Select(object.ByDomain(dom), object.ByName(app1.Name())),
			app1,
			"",
		},
		{
			"happy path by namespace and uuid",
			ctx,
			service.FirstKindVersion(),
			object.Select(object.ByNamespace(ns), object.ByUUID(svc1.UUID())),
			svc1,
			"",
		},
		{
			"happy path by namespace-qualified name",
			ctx,
			service.FirstKindVersion(),
			object.Select(object.ByNamespace(ns), object.ByName(svc1.Name())),
			svc1,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.ObjectRead(c.ctx, c.kv, c.sel)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err, err)
				require.NotNil(got)
				require.Equal(c.exp.KindVersion(), got.KindVersion())
				if c.exp.Domain() != nil {
					require.NotNil(got.Domain())
					require.Equal(c.exp.Domain().Name(), got.Domain().Name())
				}
				if c.exp.Namespace() != nil {
					require.NotNil(got.Namespace())
					require.Equal(c.exp.Namespace().Name(), got.Namespace().Name())
				}
				require.Equal(c.exp.Name(), got.Name())
				require.Equal(c.exp.UUID(), got.UUID())
				// TODO(jaypipes): finish coding Object.Diff
				//require.Equal(got, c.exp)
			}
		})
	}
}

func TestObjectWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, platform.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, platform.FirstMeta())
	require.Nil(t, err)

	domain := fixtures.Domain
	err = testutil.DomainCreateIfNotExists(ctx, rxp, domain)
	require.Nil(t, err)

	ns := fixtures.Namespace
	err = testutil.NamespaceCreateIfNotExists(ctx, rxp, ns)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, application.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, application.FirstMeta())
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, service.FirstMeta())
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

	// NOTE: Service is NamescopeNamespace which allows us to test the
	// namespace-qualified name constraints.
	svc1 := object.New(
		object.WithKindVersion(service.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithNamespace(ns),
		object.WithName(testutil.RandomName()),
	)
	svc1Name := svc1.Name()
	svcDuplicateName := object.New(
		object.WithKindVersion(service.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithNamespace(ns),
		object.WithName(svc1Name),
	)

	ctxMissingIdent := context.TODO()

	svcMissingUUID := object.New(
		object.WithKindVersion(service.FirstKindVersion()),
		object.WithNamespace(ns),
		object.WithName(testutil.RandomName()),
	)
	svcMissingName := object.New(
		object.WithKindVersion(service.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithNamespace(ns),
	)
	svcMissingNamespace := object.New(
		object.WithKindVersion(service.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithName(testutil.RandomName()),
	)

	cases := []struct {
		name    string
		ctx     context.Context
		subject *object.Object
		exp     *object.Object
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownObject,
			nil,
			"missing identity",
		},
		{
			"missing uuid",
			ctx,
			svcMissingUUID,
			nil,
			"invalid object: uuid required",
		},
		{
			"missing name",
			ctx,
			svcMissingName,
			nil,
			"invalid object: name required",
		},
		{
			"domain required",
			ctx,
			appMissingDomain,
			nil,
			"invalid object: domain required",
		},
		{
			"namespace required",
			ctx,
			svcMissingNamespace,
			nil,
			"invalid object: namespace required",
		},
		{
			"unknown kind version",
			ctx,
			fixtures.UnknownObject,
			nil,
			"unknown kind version",
		},
		{
			"happy path system-scoped object",
			ctx,
			plat1,
			plat1,
			"",
		},
		{
			"system-qualified name collision",
			ctx,
			platDuplicateName,
			nil,
			"conflict: \"platform.testing.rxp\" already exists with name",
		},
		{
			"happy path domain-scoped object",
			ctx,
			app1,
			app1,
			"",
		},
		{
			"domain-qualified name collision",
			ctx,
			appDuplicateName,
			nil,
			"conflict: \"application.testing.rxp\" already exists with name",
		},
		{
			"happy path namespace-scoped object",
			ctx,
			svc1,
			svc1,
			"",
		},
		{
			"namespace-qualified name collision",
			ctx,
			svcDuplicateName,
			nil,
			"conflict: \"service.testing.rxp\" already exists with name",
		},
		// Attempting to write the exact same object without specifying an
		// expected generation should result in a precondition failed.
		{
			"duplicate UUID",
			ctx,
			svc1,
			svc1,
			"not to exist",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := rxp.ObjectWrite(c.ctx, c.subject)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}

func TestObjectList(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, platform.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, platform.FirstMeta())
	require.Nil(t, err)

	// NOTE: Platform is NamescopeSystem which allows us to test the
	// system-qualified name constraints.
	plat1 := object.New(
		object.WithKindVersion(platform.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithName(testutil.RandomName()),
	)
	err = testutil.ObjectCreateIfNotExists(ctx, rxp, plat1)
	require.Nil(t, err)

	domain := fixtures.Domain
	err = testutil.DomainCreateIfNotExists(ctx, rxp, domain)
	require.Nil(t, err)

	ns := fixtures.Namespace
	err = testutil.NamespaceCreateIfNotExists(ctx, rxp, ns)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, application.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, application.FirstMeta())
	require.Nil(t, err)

	// NOTE: Application is NamescopeDomain which allows us to test the
	// domain-qualified name constraints.
	app1 := object.New(
		object.WithKindVersion(application.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithName(testutil.RandomName()),
	)
	err = testutil.ObjectCreateIfNotExists(ctx, rxp, app1)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, service.FirstMeta())
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
	err = testutil.ObjectCreateIfNotExists(ctx, rxp, svc1)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name             string
		ctx              context.Context
		expr             types.Expression
		opts             []option.Option
		expNumObjs       int
		expOnlyKindNames []api.KindName
		expOptions       option.Options
		expMarker        string
		expErr           string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			expression.KindNameEqual(platform.KindName),
			nil,
			0,
			nil,
			option.New(),
			"",
			"missing identity",
		},
		{
			"expression required",
			ctx,
			nil,
			nil,
			0,
			nil,
			option.New(),
			"",
			"expression required",
		},
		{
			"at least one kind is required",
			ctx,
			expression.DomainNameEqual(domain.Name()),
			nil,
			0,
			nil,
			option.New(),
			"",
			"invalid list expression: at least one kind required",
		},
		{
			"list system-qualified objects limit of 1",
			ctx,
			expression.KindNameEqual(platform.KindName),
			[]option.Option{
				option.WithLimit(1),
			},
			1,
			[]api.KindName{
				platform.KindName,
			},
			option.New(option.WithLimit(1)),
			"",
			"",
		},
		{
			"list domain-qualified objects limit of 1",
			ctx,
			expression.KindNameEqual(application.KindName),
			[]option.Option{
				option.WithLimit(1),
			},
			1,
			[]api.KindName{
				application.KindName,
			},
			option.New(option.WithLimit(1)),
			"",
			"",
		},
		{
			"list namespace-qualified objects limit of 1",
			ctx,
			expression.KindNameEqual(service.KindName),
			[]option.Option{
				option.WithLimit(1),
			},
			1,
			[]api.KindName{
				service.KindName,
			},
			option.New(option.WithLimit(1)),
			"",
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.ObjectList(c.ctx, c.expr, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err, err)
				require.NotNil(got)
				gotObjs := got.Items()
				gotOptions := got.Options()
				gotMarker := got.Marker()
				require.Equal(c.expOptions, gotOptions)
				require.Equal(c.expMarker, gotMarker)
				require.Len(gotObjs, c.expNumObjs)
				gotKindNames := lo.Map(gotObjs, func(o *object.Object, _ int) api.KindName {
					return o.KindVersion().Kind()
				})
				gotKindNames = lo.Uniq(gotKindNames)
				require.Equal(c.expOnlyKindNames, gotKindNames)
			}
		})
	}
}
