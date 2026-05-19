package driver_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/meta/read/selector"
	writeoption "github.com/relexec/rxp/meta/write/option"
	readoption "github.com/relexec/rxp/read/option"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/author"
	"github.com/relexec/rxp/testing/fixtures/book"
	"github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestMetaRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, book.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, book.FirstMeta())
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name   string
		ctx    context.Context
		sel    selector.Selector
		opts   []readoption.Option
		exp    types.Meta
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			selector.New(selector.WithKindVersion(fixtures.UnknownKindVersion)),
			nil,
			nil,
			"missing identity",
		},
		{
			"unknown kind version",
			ctx,
			selector.New(selector.WithKindVersion(fixtures.UnknownKindVersion)),
			nil,
			nil,
			"unknown kind version",
		},
		{
			"invalid kind version",
			ctx,
			selector.New(selector.WithKindVersion(fixtures.InvalidKindVersion)),
			nil,
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"happy path",
			ctx,
			selector.New(selector.WithKindVersion(book.FirstKindVersion())),
			nil,
			book.Meta_V1_0_0,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.MetaRead(c.ctx, c.sel, c.opts...)
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
	err = testutil.KindCreateIfNotExists(ctx, rxp, author.Kind)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, book.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, book.FirstMeta())
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name    string
		ctx     context.Context
		subject types.Meta
		opts    []writeoption.Option
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownMeta,
			nil,
			"missing identity",
		},
		{
			"invalid meta",
			ctx,
			fixtures.InvalidMeta,
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"duplicate meta",
			ctx,
			book.FirstMeta(),
			nil,
			"precondition failed: expected \"book.testing.rxp@1.0.0\" not to exist",
		},
		{
			"expected first version in series",
			ctx,
			author.LastMeta(),
			nil,
			"precondition failed: expected \"author.testing.rxp@1.0.1\" to have minor and patch version of 0",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := rxp.MetaWrite(c.ctx, c.subject, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
