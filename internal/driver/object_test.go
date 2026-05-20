package driver_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	testutil "github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/list/expression"
	"github.com/relexec/rxp/list/option"
	"github.com/relexec/rxp/object"
	readoption "github.com/relexec/rxp/object/read/option"
	"github.com/relexec/rxp/object/read/selector"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/application"
	"github.com/relexec/rxp/testing/fixtures/book"
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

	err = testutil.KindCreateIfNotExists(ctx, rxp, book.Kind)
	require.Nil(t, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, book.FirstMeta())
	require.Nil(t, err)

	domain := fixtures.Domain
	err = testutil.DomainCreateIfNotExists(ctx, rxp, domain)
	require.Nil(t, err)

	ns := fixtures.Namespace
	err = testutil.NamespaceCreateIfNotExists(ctx, rxp, ns)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	book1 := object.New(
		object.WithKindVersion(book.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
		object.WithName(testutil.RandomName()),
	)

	err = testutil.ObjectCreateIfNotExists(ctx, rxp, book1)
	require.Nil(t, err)

	cases := []struct {
		name   string
		ctx    context.Context
		sel    selector.Selector
		opts   []readoption.Option
		exp    types.Object
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			selector.New(
				selector.WithKindVersion(fixtures.UnknownKindVersion),
				selector.WithNamespace(book1.Namespace()),
				selector.WithUUID(book1.UUID()),
			),
			nil,
			nil,
			"missing identity",
		},
		{
			"unknown kind version",
			ctx,
			selector.New(
				selector.WithKindVersion(fixtures.UnknownKindVersion),
				selector.WithNamespace(book1.Namespace()),
				selector.WithUUID(book1.UUID()),
			),
			nil,
			nil,
			"unknown kind version",
		},
		{
			"invalid kind version",
			ctx,
			selector.New(
				selector.WithKindVersion(fixtures.InvalidKindVersion),
				selector.WithNamespace(book1.Namespace()),
				selector.WithUUID(book1.UUID()),
			),
			nil,
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"kind version required in selector",
			ctx,
			selector.New(),
			nil,
			nil,
			"invalid selector: kindversion required",
		},
		{
			"missing domain with name",
			ctx,
			selector.New(
				selector.WithKindVersion(application.FirstKindVersion()),
				selector.WithName(testutil.RandomName()),
			),
			nil,
			nil,
			"invalid selector: domain required",
		},
		{
			"missing namespace with name",
			ctx,
			selector.New(
				selector.WithKindVersion(service.FirstKindVersion()),
				selector.WithName(testutil.RandomName()),
			),
			nil,
			nil,
			"invalid selector: namespace required",
		},
		{
			"mismatched kind",
			ctx,
			selector.New(
				selector.WithKindVersion(application.FirstKindVersion()),
				selector.WithDomain(book1.Domain()),
				selector.WithUUID(book1.UUID()),
			),
			nil,
			nil,
			"not found",
		},
		{
			"unknown generation",
			ctx,
			selector.New(
				selector.WithKindVersion(book.FirstKindVersion()),
				selector.WithNamespace(book1.Namespace()),
				selector.WithUUID(book1.UUID()),
				selector.WithGeneration(42),
			),
			nil,
			nil,
			"not found",
		},
		{
			"happy path by uuid",
			ctx,
			selector.New(
				selector.WithKindVersion(book.FirstKindVersion()),
				selector.WithNamespace(book1.Namespace()),
				selector.WithUUID(book1.UUID()),
			),
			nil,
			book1,
			"",
		},
		{
			"happy path by namespace-qualified name",
			ctx,
			selector.New(
				selector.WithKindVersion(book.FirstKindVersion()),
				selector.WithNamespace(book1.Namespace()),
				selector.WithName(book1.Name()),
			),
			nil,
			book1,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.ObjectRead(c.ctx, c.sel, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err, err)
				require.NotNil(got)
				require.Equal(c.exp.KindVersion(), got.KindVersion())
				if c.exp.Domain() != nil {
					require.NotNil(got.Domain())
					require.Equal(c.exp.Domain().Name(), got.Domain().Name())
				} else {
					require.Nil(got.Domain())
				}
				if c.exp.Namespace() != nil {
					require.NotNil(got.Namespace())
					require.Equal(c.exp.Namespace().Name(), got.Namespace().Name())
				} else {
					require.Nil(got.Namespace())
				}
				require.Equal(c.exp.Name(), got.Name())
				require.Equal(c.exp.UUID(), got.UUID())
				// TODO(jaypipes): finish coding Object.Diff
				//require.Equal(got, c.exp)
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
		expOnlyKindNames []types.KindName
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
			[]types.KindName{
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
			[]types.KindName{
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
			[]types.KindName{
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
				gotKindNames := lo.Map(gotObjs, func(o types.Object, _ int) types.KindName {
					return o.KindVersion().Kind()
				})
				gotKindNames = lo.Uniq(gotKindNames)
				require.Equal(c.expOnlyKindNames, gotKindNames)
			}
		})
	}
}
