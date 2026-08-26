package driver_test

import (
	"context"
	"sort"
	"testing"

	"github.com/google/uuid"
	testutil "github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp-testing/fixtures"
	"github.com/relexec/rxp-testing/fixtures/application"
	"github.com/relexec/rxp-testing/fixtures/platform"
	"github.com/relexec/rxp-testing/fixtures/service"
	apidomain "github.com/relexec/rxp/api/domain"
	apikind "github.com/relexec/rxp/api/kind"
	apikindversion "github.com/relexec/rxp/api/kindversion"
	apiobject "github.com/relexec/rxp/api/object"
	"github.com/relexec/rxp/query"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestObjectRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, platform.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, platform.FirstKindVersion())
	require.Nil(t, err, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, application.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, application.FirstKindVersion())
	require.Nil(t, err, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, service.FirstKindVersion())
	require.Nil(t, err, err)

	dom := fixtures.Domain
	err = testutil.DomainCreateIfNotExists(ctx, rxp, dom)
	require.Nil(t, err, err)

	ctxMissingIdent := context.TODO()

	app1 := &apiobject.Object{
		KindVersionName: application.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Domain:          &dom,
		Name:            testutil.RandomName(),
	}

	err = testutil.ObjectCreateIfNotExists(ctx, rxp, app1)
	require.Nil(t, err, err)

	err = testutil.DomainCreateIfNotExists(ctx, rxp, fixtures.DomainTree_Root)
	require.Nil(t, err, err)

	err = testutil.DomainCreateIfNotExists(ctx, rxp, fixtures.DomainTree_Group1)
	require.Nil(t, err, err)

	err = testutil.DomainCreateIfNotExists(ctx, rxp, fixtures.DomainTree_Group1Leaf1)
	require.Nil(t, err, err)

	err = testutil.DomainCreateIfNotExists(ctx, rxp, fixtures.DomainTree_Group1Leaf2)
	require.Nil(t, err, err)

	err = testutil.DomainCreateIfNotExists(ctx, rxp, fixtures.DomainTree_Group2)
	require.Nil(t, err, err)

	err = testutil.DomainCreateIfNotExists(ctx, rxp, fixtures.DomainTree_Group2Leaf1)
	require.Nil(t, err, err)

	err = testutil.DomainCreateIfNotExists(ctx, rxp, fixtures.DomainTree_Group2Leaf2)
	require.Nil(t, err, err)

	svc1 := &apiobject.Object{
		KindVersionName: service.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Domain:          &fixtures.DomainTree_Group1,
		Name:            testutil.RandomName(),
	}

	err = testutil.ObjectCreateIfNotExists(ctx, rxp, svc1)
	require.Nil(t, err, err)

	cases := []struct {
		name   string
		ctx    context.Context
		kv     apikindversion.Name
		sel    apiobject.Selector
		exp    *apiobject.Object
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			service.FirstKindVersionName(),
			apiobject.Select(apiobject.ByUUID(svc1.UUID)),
			nil,
			"missing identity",
		},
		{
			"unknown kind",
			ctx,
			fixtures.UnknownKindVersionName,
			apiobject.Select(apiobject.ByUUID(svc1.UUID)),
			nil,
			"unknown kind",
		},
		{
			"invalid kind version",
			ctx,
			fixtures.InvalidKindVersionName,
			apiobject.Select(apiobject.ByUUID(svc1.UUID)),
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"either uuid or name required in selector",
			ctx,
			service.FirstKindVersionName(),
			apiobject.Select(),
			nil,
			"invalid selector: uuid or name required",
		},
		{
			"missing domain with name",
			ctx,
			application.FirstKindVersionName(),
			apiobject.Select(apiobject.ByName(testutil.RandomName())),
			nil,
			"invalid selector: domain required",
		},
		{
			"mismatched kind",
			ctx,
			application.FirstKindVersionName(),
			apiobject.Select(
				apiobject.ByDomain(&dom),
				apiobject.ByUUID(svc1.UUID),
			),
			nil,
			"not found",
		},
		{
			"unknown generation",
			ctx,
			application.FirstKindVersionName(),
			apiobject.Select(
				apiobject.ByDomain(&dom),
				apiobject.ByUUID(app1.UUID),
				apiobject.ByGeneration(42),
			),
			nil,
			"not found",
		},
		{
			"missing domain when domain-scoped",
			ctx,
			application.FirstKindVersionName(),
			apiobject.Select(
				apiobject.ByUUID(app1.UUID),
			),
			nil,
			"invalid selector: domain required",
		},
		{
			"happy path domain-scoped with uuid and root domain",
			ctx,
			application.FirstKindVersionName(),
			apiobject.Select(
				apiobject.ByDomain(&dom),
				apiobject.ByUUID(app1.UUID),
			),
			app1,
			"",
		},
		{
			"happy path domain-scoped by name with root domain",
			ctx,
			application.FirstKindVersionName(),
			apiobject.Select(
				apiobject.ByDomain(&dom),
				apiobject.ByName(app1.Name),
			),
			app1,
			"",
		},
		{
			"happy path domain-scoped with uuid and parent domain",
			ctx,
			service.FirstKindVersionName(),
			apiobject.Select(
				apiobject.ByDomain(&fixtures.DomainTree_Group1),
				apiobject.ByUUID(svc1.UUID),
			),
			svc1,
			"",
		},
		{
			"happy path domain-scoped by name with parent domain",
			ctx,
			service.FirstKindVersionName(),
			apiobject.Select(
				apiobject.ByDomain(&fixtures.DomainTree_Group1),
				apiobject.ByName(svc1.Name),
			),
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
				require.Equal(c.exp.KindVersionName, got.KindVersionName)
				if c.exp.Domain != nil {
					require.NotNil(got.Domain)
					require.Equal(c.exp.Domain.Name, got.Domain.Name)
				}
				require.Equal(c.exp.Name, got.Name)
				require.Equal(c.exp.UUID, got.UUID)
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

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, platform.FirstKindVersion())
	require.Nil(t, err)

	dom := fixtures.Domain
	err = testutil.DomainCreateIfNotExists(ctx, rxp, dom)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, application.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, application.FirstKindVersion())
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, service.FirstKindVersion())
	require.Nil(t, err)

	// NOTE: Platform is ScopeSystem which allows us to test the
	// system-qualified name constraints.
	plat1 := &apiobject.Object{
		KindVersionName: platform.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Name:            testutil.RandomName(),
	}
	plat1Name := plat1.Name
	platDuplicateName := &apiobject.Object{
		KindVersionName: platform.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Name:            plat1Name,
	}
	plat1Gen1 := plat1.Clone()
	plat1Gen1.Generation = 1

	// NOTE: Application is ScopeDomain which allows us to test the
	// domain-qualified name constraints.
	appMissingDomain := &apiobject.Object{
		KindVersionName: application.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Name:            testutil.RandomName(),
	}
	app1 := &apiobject.Object{
		KindVersionName: application.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Domain:          &dom,
		Name:            testutil.RandomName(),
	}
	app1Name := app1.Name
	appDuplicateName := &apiobject.Object{
		KindVersionName: application.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Domain:          &dom,
		Name:            app1Name,
	}
	app1Gen1 := app1.Clone()
	app1Gen1.Generation = 1

	// We test domain hierarchy management with the Service object by using a
	// Domain with a parent Domain.
	svc1 := &apiobject.Object{
		KindVersionName: service.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Domain:          &fixtures.DomainTree_Group1,
		Name:            testutil.RandomName(),
	}
	svc1Name := svc1.Name
	svcDuplicateName := &apiobject.Object{
		KindVersionName: service.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Domain:          &fixtures.DomainTree_Group1,
		Name:            svc1Name,
	}

	ctxMissingIdent := context.TODO()

	svcMissingUUID := &apiobject.Object{
		KindVersionName: service.FirstKindVersionName(),
		Name:            testutil.RandomName(),
	}
	svcMissingName := &apiobject.Object{
		KindVersionName: service.FirstKindVersionName(),
		UUID:            uuid.NewString(),
	}

	svc1Gen1 := svc1.Clone()
	svc1Gen1.Generation = 1
	svc1Gen2 := svc1.Clone()
	svc1Gen2.Generation = 2

	cases := []struct {
		name    string
		ctx     context.Context
		subject *apiobject.Object
		exp     *apiobject.Object
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			&fixtures.UnknownObject,
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
			"unknown kind",
			ctx,
			&fixtures.UnknownObject,
			nil,
			"unknown kind",
		},
		{
			"happy path system-scoped object",
			ctx,
			plat1,
			&plat1Gen1,
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
			"happy path domain-scoped object root domain",
			ctx,
			app1,
			&app1Gen1,
			"",
		},
		{
			"domain-qualified name collision root domain",
			ctx,
			appDuplicateName,
			nil,
			"conflict: \"application.testing.rxp\" already exists with name",
		},
		{
			"happy path domain-scoped object parent domain",
			ctx,
			svc1,
			&svc1Gen1,
			"",
		},
		{
			"domain-qualified name collision parent domain",
			ctx,
			svcDuplicateName,
			nil,
			"conflict: \"service.testing.rxp\" already exists with name",
		},
		// Attempting to write the exact same object without specifying the
		// existing generation should result in a precondition failed.
		{
			"duplicate UUID",
			ctx,
			svc1,
			nil,
			"not to exist",
		},
		{
			"new generation",
			ctx,
			&svc1Gen1,
			&svc1Gen2,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.ObjectWrite(c.ctx, *c.subject)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
				require.Nil(got)
			} else {
				require.Nil(err, err)
				require.NotNil(got)
				require.Equal(c.exp.KindVersionName, got.KindVersionName)
				if c.exp.Domain != nil {
					require.NotNil(got.Domain)
					require.Equal(c.exp.Domain.Name, got.Domain.Name)
				}
				require.Equal(c.exp.Name, got.Name)
				require.Equal(c.exp.UUID, got.UUID)
				require.Equal(c.exp.Generation, got.Generation)
				// TODO(jaypipes): finish coding Object.Diff
				//require.Equal(got, c.exp)
			}
		})
	}
}

func TestObjectQuery(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, platform.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, platform.FirstKindVersion())
	require.Nil(t, err)

	// NOTE: Platform is NamescopeSystem which allows us to test the
	// system-qualified name constraints.
	plat1 := &apiobject.Object{
		KindVersionName: platform.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Name:            testutil.RandomName(),
	}
	err = testutil.ObjectCreateIfNotExists(ctx, rxp, plat1)
	require.Nil(t, err)

	dom := fixtures.Domain
	err = testutil.DomainCreateIfNotExists(ctx, rxp, dom)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, application.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, application.FirstKindVersion())
	require.Nil(t, err)

	// NOTE: Application is NamescopeDomain which allows us to test the
	// domain-qualified name constraints.
	app1 := &apiobject.Object{
		KindVersionName: application.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Domain:          &dom,
		Name:            testutil.RandomName(),
	}
	err = testutil.ObjectCreateIfNotExists(ctx, rxp, app1)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, service.FirstKindVersion())
	require.Nil(t, err)

	svc1 := &apiobject.Object{
		KindVersionName: service.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Domain:          &dom,
		Name:            testutil.RandomName(),
	}
	err = testutil.ObjectCreateIfNotExists(ctx, rxp, svc1)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name             string
		ctx              context.Context
		kv               apikindversion.Name
		expr             query.Expression
		opts             []query.Option
		expNumObjs       int
		expOnlyKindNames []apikind.Name
		expOptionLimit   uint
		expMarkerEmpty   bool
		expErr           string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			apikindversion.Name(platform.KindName),
			nil,
			nil,
			0,
			nil,
			0,
			true,
			"missing identity",
		},
		{
			"invalid kindversion",
			ctx,
			apikindversion.Name(fixtures.InvalidKindName),
			apidomain.NameEqual(dom.Name),
			nil,
			0,
			nil,
			0,
			true,
			"invalid kind name",
		},
		{
			"invalid query expression kind predicate",
			ctx,
			apikindversion.Name(platform.KindName),
			apikind.NameEqual(application.KindName),
			nil,
			0,
			nil,
			0,
			true,
			"invalid query expression: kind predicate not allowed",
		},
		{
			"query system-qualified objects limit of 1",
			ctx,
			apikindversion.Name(platform.KindName),
			nil,
			[]query.Option{
				query.Limit(1),
			},
			1,
			[]apikind.Name{
				platform.KindName,
			},
			1,
			false,
			"",
		},
		{
			"query domain-qualified objects limit of 1",
			ctx,
			apikindversion.Name(application.KindName),
			nil,
			[]query.Option{
				query.Limit(1),
			},
			1,
			[]apikind.Name{
				application.KindName,
			},
			1,
			false,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)

			got, err := rxp.ObjectQuery(c.ctx, c.kv, c.expr, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err, err)
				require.NotNil(got)
				gotObjs := got.Items()
				gotOptions := got.Options()
				gotMarker := got.Marker()
				require.Equal(c.expOptionLimit, gotOptions.Limit())
				require.Equal(c.expMarkerEmpty, gotMarker == "")
				require.Len(gotObjs, c.expNumObjs)
				gotKindNames := lo.Map(gotObjs, func(o *apiobject.Object, _ int) apikind.Name {
					return o.KindName()
				})
				gotKindNames = lo.Uniq(gotKindNames)
				require.Equal(c.expOnlyKindNames, gotKindNames)
			}
		})
	}
}

func TestObjectQuery_SystemQualified(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, platform.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, platform.FirstKindVersion())
	require.Nil(t, err)

	// NOTE: Platform is NamescopeSystem which allows us to test the
	// system-qualified name constraints.
	plat1 := &apiobject.Object{
		KindVersionName: platform.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Name:            testutil.RandomName(),
	}
	err = testutil.ObjectCreateIfNotExists(ctx, rxp, plat1)
	require.Nil(t, err)

	plat2 := &apiobject.Object{
		KindVersionName: platform.FirstKindVersionName(),
		UUID:            uuid.NewString(),
		Name:            testutil.RandomName(),
	}
	err = testutil.ObjectCreateIfNotExists(ctx, rxp, plat2)
	require.Nil(t, err)

	cases := []struct {
		name     string
		ctx      context.Context
		kv       apikindversion.Name
		expr     query.Expression
		opts     []query.Option
		expUUIDs []string
		expErr   string
	}{
		{
			"by UUID",
			ctx,
			apikindversion.Name(platform.KindName),
			apiobject.UUIDEqual(plat1.UUID),
			[]query.Option{
				query.Limit(1),
			},
			[]string{plat1.UUID},
			"",
		},
		{
			"in set of UUIDs",
			ctx,
			apikindversion.Name(platform.KindName),
			apiobject.UUIDIn(plat1.UUID, plat2.UUID),
			[]query.Option{
				query.Limit(2),
			},
			[]string{plat1.UUID, plat2.UUID},
			"",
		},
		{
			"by name",
			ctx,
			apikindversion.Name(platform.KindName),
			apiobject.NameEqual(plat1.Name),
			[]query.Option{
				query.Limit(1),
			},
			[]string{plat1.UUID},
			"",
		},
		{
			"in set of names",
			ctx,
			apikindversion.Name(platform.KindName),
			apiobject.NameIn(plat1.Name, plat2.Name),
			[]query.Option{
				query.Limit(2),
			},
			[]string{plat1.UUID, plat2.UUID},
			"",
		},
		{
			"OR expression UUIDs",
			ctx,
			apikindversion.Name(platform.KindName),
			query.Or(
				apiobject.UUIDEqual(plat1.UUID),
				apiobject.UUIDEqual(plat2.UUID),
			),
			[]query.Option{
				query.Limit(2),
			},
			[]string{plat1.UUID, plat2.UUID},
			"",
		},
		{
			"OR expression names",
			ctx,
			apikindversion.Name(platform.KindName),
			query.Or(
				apiobject.NameEqual(plat1.Name),
				apiobject.NameEqual(plat2.Name),
			),
			[]query.Option{
				query.Limit(2),
			},
			[]string{plat1.UUID, plat2.UUID},
			"",
		},
		{
			"OR expression uuid and name",
			ctx,
			apikindversion.Name(platform.KindName),
			query.Or(
				apiobject.UUIDEqual(plat1.UUID),
				apiobject.NameEqual(plat2.Name),
			),
			[]query.Option{
				query.Limit(2),
			},
			[]string{plat1.UUID, plat2.UUID},
			"",
		},
		{
			"OR expression uuid and name and unknown",
			ctx,
			apikindversion.Name(platform.KindName),
			query.Or(
				apiobject.UUIDEqual(plat1.UUID),
				apiobject.NameEqual(plat2.Name),
				apiobject.UUIDEqual(uuid.NewString()),
			),
			[]query.Option{
				query.Limit(2),
			},
			[]string{plat1.UUID, plat2.UUID},
			"",
		},
		{
			"AND expression UUIDs",
			ctx,
			apikindversion.Name(platform.KindName),
			query.And(
				apiobject.UUIDEqual(plat1.UUID),
				apiobject.UUIDEqual(plat2.UUID),
			),
			[]query.Option{
				query.Limit(2),
			},
			[]string{},
			"",
		},
		{
			"AND expression uuid and name",
			ctx,
			apikindversion.Name(platform.KindName),
			query.And(
				apiobject.UUIDEqual(plat1.UUID),
				apiobject.NameEqual(plat1.Name),
			),
			[]query.Option{
				query.Limit(2),
			},
			[]string{plat1.UUID},
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)

			got, err := rxp.ObjectQuery(c.ctx, c.kv, c.expr, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err, err)
				require.NotNil(got)
				gotObjs := got.Items()
				expUUIDs := c.expUUIDs
				expNumObjs := len(c.expUUIDs)
				require.Len(gotObjs, expNumObjs)
				gotUUIDs := lo.Map(gotObjs, func(o *apiobject.Object, _ int) string {
					return o.UUID
				})
				sort.Strings(expUUIDs)
				sort.Strings(gotUUIDs)
				require.Equal(expUUIDs, gotUUIDs)
			}
		})
	}
}
