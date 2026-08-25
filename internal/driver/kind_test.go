package driver_test

import (
	"context"
	"testing"

	"github.com/relexec/delta/fieldpath"
	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp-testing/fixtures"
	"github.com/relexec/rxp-testing/fixtures/service"
	apikind "github.com/relexec/rxp/api/kind"
	apiobject "github.com/relexec/rxp/api/object"
	apisystem "github.com/relexec/rxp/api/system"
	"github.com/relexec/rxp/query"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestKindRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name   string
		ctx    context.Context
		sel    apikind.Selector
		exp    *apikind.Kind
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			apikind.Select(apikind.ByName(fixtures.InvalidKindName)),
			nil,
			"missing identity",
		},
		{
			"invalid kind",
			ctx,
			apikind.Select(apikind.ByName(fixtures.InvalidKindName)),
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"unknown kind",
			ctx,
			apikind.Select(apikind.ByName(fixtures.UnknownKindName)),
			nil,
			"not found",
		},
		{
			"happy path",
			ctx,
			apikind.Select(apikind.ByName(service.KindName)),
			&service.Kind,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.KindRead(c.ctx, c.sel)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
				delta, err := apikind.Diff(*c.exp, got)
				require.Nil(err)
				require.False(
					delta.DifferentExcept(
						fieldpath.FromString("system"),
					),
					delta.Differences(),
				)
			}
		})
	}
}

func TestKindWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name    string
		ctx     context.Context
		subject apikind.Kind
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownKind,
			"missing identity",
		},
		{
			"invalid kind",
			ctx,
			fixtures.InvalidKind,
			"invalid kind name: invalid characters",
		},
		{
			"duplicate kind",
			ctx,
			service.Kind,
			"conflict: \"kind\" already exists",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := rxp.KindWrite(c.ctx, c.subject)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}

func TestKindQuery(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name         string
		ctx          context.Context
		expr         query.Expression
		opts         []query.Option
		expNumItems  int
		expOnlyUUIDs []string
		expOptions   query.Options
		expMarker    string
		expErr       string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			apikind.UUIDEqual(service.KindUUID),
			nil,
			0,
			nil,
			query.Options{},
			"",
			"missing identity",
		},
		{
			"unsupported predicate",
			ctx,
			apiobject.GenerationEqual(0),
			nil,
			0,
			nil,
			query.Options{},
			"",
			"unsupported predicate apiobject.GenerationPredicate",
		},
		{
			"expression required",
			ctx,
			nil,
			nil,
			0,
			nil,
			query.Options{},
			"",
			"expression required",
		},
		{
			"unsupported expression",
			ctx,
			query.Or(
				apikind.NameEqual(service.KindName),
				apikind.NameEqual(fixtures.UnknownKindName),
			),
			nil,
			0,
			nil,
			query.Options{},
			"",
			"unsupported expression query.OrExpression",
		},
		{
			"no results when looking up non-existing kind UUID",
			ctx,
			apikind.UUIDEqual(fixtures.UnknownKindUUID),
			nil,
			0,
			[]string{},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"no results when looking up non-existing kind name",
			ctx,
			apikind.NameEqual(fixtures.UnknownKindName),
			nil,
			0,
			[]string{},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"no results when looking up kinds by non-existing system",
			ctx,
			apisystem.Equal(&fixtures.UnknownSystem),
			nil,
			0,
			[]string{},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"no results when looking up kinds by non-existing system UUID",
			ctx,
			apisystem.UUIDEqual(fixtures.UnknownSystemUUID),
			nil,
			0,
			[]string{},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query kinds by name, expect one",
			ctx,
			apikind.NameEqual(service.KindName),
			nil,
			1,
			[]string{
				service.KindUUID,
			},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query kinds by UUID, expect one",
			ctx,
			apikind.UUIDEqual(service.KindUUID),
			nil,
			1,
			[]string{
				service.KindUUID,
			},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query kinds by UUID in, expect one",
			ctx,
			apikind.UUIDIn(service.KindUUID, fixtures.UnknownKindUUID),
			nil,
			1,
			[]string{
				service.KindUUID,
			},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query kinds by kind UUID, expect one",
			ctx,
			apikind.UUIDEqual(service.KindUUID),
			nil,
			1,
			[]string{
				service.KindUUID,
			},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)

			got, err := rxp.KindQuery(c.ctx, c.expr, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err, err)
				require.NotNil(got)
				gotItems := got.Items()
				gotOptions := got.Options()
				gotMarker := got.Marker()
				require.Equal(c.expOptions, gotOptions)
				require.Equal(c.expMarker, gotMarker)
				require.Len(gotItems, c.expNumItems)
				gotUUIDs := lo.Map(gotItems, func(k *apikind.Kind, _ int) string {
					return k.UUID
				})
				gotUUIDs = lo.Uniq(gotUUIDs)
				require.Equal(c.expOnlyUUIDs, gotUUIDs)
				for _, item := range gotItems {
					require.NotNil(item.System)
				}
			}
		})
	}
}
