package driver_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp-testing/fixtures"
	"github.com/relexec/rxp-testing/fixtures/service"
	rxpkind "github.com/relexec/rxp/kind"
	rxpkindversion "github.com/relexec/rxp/kindversion"
	rxpobject "github.com/relexec/rxp/object"
	rxpquery "github.com/relexec/rxp/query"
	rxpsystem "github.com/relexec/rxp/system"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestKindVersionRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, service.FirstKindVersion())
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name   string
		ctx    context.Context
		sel    rxpkindversion.Selector
		exp    *rxpkindversion.KindVersion
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			rxpkindversion.Select(
				rxpkindversion.ByName(fixtures.UnknownKindVersionName),
			),
			nil,
			"missing identity",
		},
		{
			"unknown kind version",
			ctx,
			rxpkindversion.Select(
				rxpkindversion.ByName(fixtures.UnknownKindVersionName),
			),
			nil,
			"unknown kind",
		},
		{
			"invalid kind version",
			ctx,
			rxpkindversion.Select(
				rxpkindversion.ByName(fixtures.InvalidKindVersionName),
			),
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"happy path",
			ctx,
			rxpkindversion.Select(
				rxpkindversion.ByName(service.FirstKindVersionName()),
			),
			service.KindVersion_V1_0_0,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.KindVersionRead(c.ctx, c.sel)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
				require.Equal(c.exp.Name(), got.Name())
				expSchema := c.exp.Schema
				gotSchema := got.Schema
				delta, err := expSchema.Diff(gotSchema)
				require.Nil(err)
				require.False(delta.Different())
			}
		})
	}
}

func TestKindVersionWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	// NOTE(jaypipes): We ensure the author Kind here but not any KindVersions
	// (KindVersions) for it. This allows us to properly test the precondition
	// failed for minor/patch version number of 0.
	err = testutil.KindCreateIfNotExists(ctx, rxp, fixtures.NoKindVersionsKind)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, service.FirstKindVersion())
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name    string
		ctx     context.Context
		subject rxpkindversion.KindVersion
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownKindVersion,
			"missing identity",
		},
		{
			"invalid kindversion",
			ctx,
			fixtures.InvalidKindVersion,
			"invalid kind name: invalid characters",
		},
		{
			"duplicate kindversion",
			ctx,
			*service.KindVersion_V1_0_0,
			"precondition failed: expected \"service.testing.rxp@1.0.0\" not to exist",
		},
		{
			"expected first version in series",
			ctx,
			fixtures.NoKindVersionsKindVersion,
			"precondition failed: expected \"nokindversions.testing.rxp@1.0.1\" to have minor and patch version of 0",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := rxp.KindVersionWrite(c.ctx, c.subject)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}

func TestKindVersionQuery(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	// NOTE(jaypipes): We ensure the author Kind here but not any KindVersions
	// (KindVersions) for it. This allows us to properly test the precondition
	// failed for minor/patch version number of 0.
	err = testutil.KindCreateIfNotExists(ctx, rxp, fixtures.NoKindVersionsKind)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, service.FirstKindVersion())
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name         string
		ctx          context.Context
		expr         rxpquery.Expression
		opts         []rxpquery.Option
		expNumItems  int
		expOnlyNames []rxpkindversion.Name
		expOptions   rxpquery.Options
		expMarker    string
		expErr       string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			rxpkindversion.NameEqual(service.FirstKindVersionName()),
			nil,
			0,
			nil,
			rxpquery.Options{},
			"",
			"missing identity",
		},
		{
			"unsupported predicate",
			ctx,
			rxpobject.GenerationEqual(0),
			nil,
			0,
			nil,
			rxpquery.Options{},
			"",
			"unsupported predicate",
		},
		{
			"expression required",
			ctx,
			nil,
			nil,
			0,
			nil,
			rxpquery.Options{},
			"",
			"expression required",
		},
		{
			"unsupported expression",
			ctx,
			rxpquery.Or(
				rxpkind.NameEqual(service.KindName),
				rxpkind.NameEqual(fixtures.UnknownKindName),
			),
			nil,
			0,
			nil,
			rxpquery.Options{},
			"",
			"unsupported expression",
		},
		{
			"no results when looking up non-existing kind version name",
			ctx,
			rxpkindversion.NameEqual(fixtures.UnknownKindVersionName),
			nil,
			0,
			[]rxpkindversion.Name{},
			rxpquery.NewOptions(
				rxpquery.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"no results when looking up kindversions by non-existing system",
			ctx,
			rxpsystem.Equal(&fixtures.UnknownSystem),
			nil,
			0,
			[]rxpkindversion.Name{},
			rxpquery.NewOptions(
				rxpquery.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"no results when looking up kindversions by non-existing system UUID",
			ctx,
			rxpsystem.UUIDEqual(fixtures.UnknownSystemUUID),
			nil,
			0,
			[]rxpkindversion.Name{},
			rxpquery.NewOptions(
				rxpquery.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query kindversions by name, expect one",
			ctx,
			rxpkindversion.NameEqual(service.FirstKindVersionName()),
			nil,
			1,
			[]rxpkindversion.Name{
				service.FirstKindVersionName(),
			},
			rxpquery.NewOptions(
				rxpquery.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query kindversions by name in set, expect one",
			ctx,
			rxpkindversion.NameIn(service.FirstKindVersionName(), fixtures.UnknownKindVersionName),
			nil,
			1,
			[]rxpkindversion.Name{
				service.FirstKindVersionName(),
			},
			rxpquery.NewOptions(
				rxpquery.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)

			got, err := rxp.KindVersionQuery(c.ctx, c.expr, c.opts...)
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
				gotNames := lo.Map(gotItems, func(kv *rxpkindversion.KindVersion, _ int) rxpkindversion.Name {
					return kv.Name()
				})
				gotNames = lo.Uniq(gotNames)
				require.Equal(c.expOnlyNames, gotNames)
				for _, item := range gotItems {
					require.NotNil(item.System)
				}
			}
		})
	}
}
