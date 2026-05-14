package store_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	testutil "github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/object/list/expression"
	listoption "github.com/relexec/rxp/object/list/option"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/application"
	"github.com/relexec/rxp/testing/fixtures/platform"
	"github.com/relexec/rxp/testing/fixtures/service"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestObjectList(t *testing.T) {
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
	err = testutil.EnsureObject(ctx, s, plat1)
	require.Nil(t, err)

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
	app1 := object.New(
		object.WithKindVersion(application.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithName(testutil.RandomName()),
	)
	err = testutil.EnsureObject(ctx, s, app1)
	require.Nil(t, err)

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
	err = testutil.EnsureObject(ctx, s, svc1)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name             string
		ctx              context.Context
		expr             rxptypes.Expression
		opts             []listoption.Option
		expNumObjs       int
		expOnlyKindNames []rxptypes.KindName
		expOptions       listoption.Options
		expMarker        string
		expErr           string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			expression.KindEqual(platform.KindName),
			nil,
			0,
			nil,
			listoption.New(),
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
			listoption.New(),
			"",
			"expression required",
		},
		{
			"at least one kind is required",
			ctx,
			expression.DomainEqual(domain.Name()),
			nil,
			0,
			nil,
			listoption.New(),
			"",
			"invalid list expression: at least one kind required",
		},
		{
			"list system-qualified objects limit of 1",
			ctx,
			expression.KindEqual(platform.KindName),
			[]listoption.Option{
				listoption.WithLimit(1),
			},
			1,
			[]rxptypes.KindName{
				platform.KindName,
			},
			listoption.New(listoption.WithLimit(1)),
			"",
			"",
		},
		{
			"list domain-qualified objects limit of 1",
			ctx,
			expression.KindEqual(application.KindName),
			[]listoption.Option{
				listoption.WithLimit(1),
			},
			1,
			[]rxptypes.KindName{
				application.KindName,
			},
			listoption.New(listoption.WithLimit(1)),
			"",
			"",
		},
		{
			"list namespace-qualified objects limit of 1",
			ctx,
			expression.KindEqual(service.KindName),
			[]listoption.Option{
				listoption.WithLimit(1),
			},
			1,
			[]rxptypes.KindName{
				service.KindName,
			},
			listoption.New(listoption.WithLimit(1)),
			"",
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := s.ObjectList(c.ctx, c.expr, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err, err)
				require.NotNil(got)
				gotObjs := got.Objects()
				gotOptions := got.Options()
				gotMarker := got.Marker()
				require.Equal(c.expOptions, gotOptions)
				require.Equal(c.expMarker, gotMarker)
				require.Len(gotObjs, c.expNumObjs)
				gotKindNames := lo.Map(gotObjs, func(o rxptypes.Object, _ int) rxptypes.KindName {
					return o.KindVersion().Kind()
				})
				gotKindNames = lo.Uniq(gotKindNames)
				require.Equal(c.expOnlyKindNames, gotKindNames)
			}
		})
	}
}
