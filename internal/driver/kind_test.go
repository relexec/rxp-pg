package driver_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/cmp/fieldpath"
	writeoption "github.com/relexec/rxp/kind/write/option"
	readoption "github.com/relexec/rxp/read/option"
	selector "github.com/relexec/rxp/read/selector/kind"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/book"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestKindRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, book.Kind)
	require.Nil(t, err, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name   string
		ctx    context.Context
		sel    selector.Selector
		opts   []readoption.Option
		exp    rxptypes.Kind
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			selector.New(selector.WithName(fixtures.InvalidKindName)),
			nil,
			nil,
			"missing identity",
		},
		{
			"unknown kind",
			ctx,
			selector.New(selector.WithName(fixtures.UnknownKindName)),
			nil,
			nil,
			"not found",
		},
		{
			"invalid kind",
			ctx,
			selector.New(selector.WithName(fixtures.InvalidKindName)),
			nil,
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"happy path",
			ctx,
			selector.New(selector.WithName(book.KindName)),
			nil,
			book.Kind,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.KindRead(c.ctx, c.sel, c.opts...)
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

	err = testutil.KindCreateIfNotExists(ctx, rxp, book.Kind)
	require.Nil(t, err, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name    string
		ctx     context.Context
		subject rxptypes.Kind
		opts    []writeoption.Option
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownKind,
			nil,
			"missing identity",
		},
		{
			"invalid kind",
			ctx,
			fixtures.InvalidKind,
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"duplicate kind",
			ctx,
			book.Kind,
			nil,
			"conflict: \"kind\" already exists",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := rxp.KindWrite(c.ctx, c.subject, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
