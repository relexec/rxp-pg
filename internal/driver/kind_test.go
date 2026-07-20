package driver_test

import (
	"context"
	"testing"

	"github.com/relexec/delta/fieldpath"
	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp-testing/fixtures"
	"github.com/relexec/rxp-testing/fixtures/service"
	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/kind"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/system"
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
		sel    kind.Selector
		exp    *api.Kind
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			kind.Select(kind.ByName(fixtures.InvalidKindName)),
			nil,
			"missing identity",
		},
		{
			"invalid kind",
			ctx,
			kind.Select(kind.ByName(fixtures.InvalidKindName)),
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"unknown kind",
			ctx,
			kind.Select(kind.ByName(fixtures.UnknownKindName)),
			nil,
			"not found",
		},
		{
			"happy path",
			ctx,
			kind.Select(kind.ByName(service.KindName)),
			service.Kind,
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
				delta, err := kind.Diff(*c.exp, got)
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
		subject *api.Kind
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
			err := rxp.KindWrite(c.ctx, *c.subject)
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
			kind.UUIDEqual(service.KindUUID),
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
			object.GenerationEqual(0),
			nil,
			0,
			nil,
			query.Options{},
			"",
			"unsupported predicate object.GenerationPredicate",
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
				kind.NameEqual(service.KindName),
				kind.NameEqual(fixtures.UnknownKindName),
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
			kind.UUIDEqual(fixtures.UnknownKindUUID),
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
			kind.NameEqual(fixtures.UnknownKindName),
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
			system.Equal(fixtures.UnknownSystem),
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
			system.UUIDEqual(fixtures.UnknownSystemUUID),
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
			kind.NameEqual(service.KindName),
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
			kind.UUIDEqual(service.KindUUID),
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
			kind.UUIDIn(service.KindUUID, fixtures.UnknownKindUUID),
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
			kind.UUIDEqual(service.KindUUID),
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
				gotUUIDs := lo.Map(gotItems, func(k *api.Kind, _ int) string {
					return k.UUID()
				})
				gotUUIDs = lo.Uniq(gotUUIDs)
				require.Equal(c.expOnlyUUIDs, gotUUIDs)
				for _, item := range gotItems {
					require.NotNil(item.System())
				}
			}
		})
	}
}
