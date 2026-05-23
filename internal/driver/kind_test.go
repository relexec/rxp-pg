package driver_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/cmp/fieldpath"
	"github.com/relexec/rxp/kind"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/service"
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
		exp    *kind.Kind
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
				delta, err := c.exp.Diff(got)
				require.Nil(err)
				require.False(
					delta.DifferentExcept(
						fieldpath.FromString("system"),
					),
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
		subject *kind.Kind
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
