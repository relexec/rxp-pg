package driver_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/meta"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/service"
	"github.com/stretchr/testify/require"
)

func TestMetaRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, service.FirstMeta())
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name   string
		ctx    context.Context
		sel    meta.Selector
		exp    *meta.Meta
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			meta.ByKindVersion(fixtures.UnknownKindVersion),
			nil,
			"missing identity",
		},
		{
			"unknown kind version",
			ctx,
			meta.ByKindVersion(fixtures.UnknownKindVersion),
			nil,
			"unknown kind version",
		},
		{
			"invalid kind version",
			ctx,
			meta.ByKindVersion(fixtures.InvalidKindVersion),
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"happy path",
			ctx,
			meta.ByKindVersion(service.FirstKindVersion()),
			service.Meta_V1_0_0,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.MetaRead(c.ctx, c.sel)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
				require.Equal(c.exp.KindVersion(), got.KindVersion())
				expSchema := c.exp.Schema()
				gotSchema := got.Schema()
				delta, err := expSchema.Diff(gotSchema)
				require.Nil(err)
				require.False(delta.Different())
			}
		})
	}
}

func TestMetaWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	// NOTE(jaypipes): We ensure the author Kind here but not any Metas
	// (KindVersions) for it. This allows us to properly test the precondition
	// failed for minor/patch version number of 0.
	err = testutil.KindCreateIfNotExists(ctx, rxp, fixtures.NoMetaKind)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, service.FirstMeta())
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name    string
		ctx     context.Context
		subject *meta.Meta
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownMeta,
			"missing identity",
		},
		{
			"invalid meta",
			ctx,
			fixtures.InvalidMeta,
			"invalid kind name: invalid characters",
		},
		{
			"duplicate meta",
			ctx,
			service.FirstMeta(),
			"precondition failed: expected \"service.testing.rxp@1.0.0\" not to exist",
		},
		{
			"expected first version in series",
			ctx,
			fixtures.NoMetaMeta,
			"precondition failed: expected \"nometa.testing.rxp@1.0.1\" to have minor and patch version of 0",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := rxp.MetaWrite(c.ctx, c.subject)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
