package driver_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/kind/kindversion"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/service"
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
		sel    kindversion.Selector
		exp    *kindversion.KindVersion
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			kindversion.Select(
				kindversion.ByName(fixtures.UnknownKindVersionName),
			),
			nil,
			"missing identity",
		},
		{
			"unknown kind version",
			ctx,
			kindversion.Select(
				kindversion.ByName(fixtures.UnknownKindVersionName),
			),
			nil,
			"unknown kind version",
		},
		{
			"invalid kind version",
			ctx,
			kindversion.Select(
				kindversion.ByName(fixtures.InvalidKindVersionName),
			),
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"happy path",
			ctx,
			kindversion.Select(
				kindversion.ByName(service.FirstKindVersionName()),
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
				expSchema := c.exp.Schema()
				gotSchema := got.Schema()
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
		subject *kindversion.KindVersion
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
			service.FirstKindVersion(),
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
