package store_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	readoption "github.com/relexec/rxp/meta/read/option"
	"github.com/relexec/rxp/meta/read/selector"
	writeoption "github.com/relexec/rxp/meta/write/option"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/author"
	bookv1 "github.com/relexec/rxp/testing/fixtures/book/v1"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestMetaRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, bookv1.Meta_V1_0_0)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name   string
		ctx    context.Context
		sel    selector.Selector
		opts   []readoption.Option
		exp    rxptypes.Meta
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
			"not found",
		},
		{
			"invalid kind version",
			ctx,
			selector.New(selector.WithKindVersion(fixtures.InvalidKindVersion)),
			nil,
			nil,
			"invalid kind: invalid characters",
		},
		{
			"happy path",
			ctx,
			selector.New(selector.WithKindVersion(bookv1.KindVersion_V1_0_0)),
			nil,
			bookv1.Meta_V1_0_0,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := s.MetaRead(c.ctx, c.sel, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
				require.Equal(c.exp.KindVersion(), got.KindVersion())
				require.Equal(c.exp.Namescope(), got.Namescope())
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
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, bookv1.Meta_V1_0_0)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name    string
		ctx     context.Context
		subject rxptypes.Meta
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
			"invalid kind: invalid characters",
		},
		{
			"duplicate meta",
			ctx,
			bookv1.Meta_V1_0_0,
			nil,
			"precondition failed: expected \"book.testing.rxp@1.0.0\" not to exist",
		},
		{
			"expected first version in series",
			ctx,
			author.LatestMeta(),
			nil,
			"precondition failed: expected \"author.testing.rxp@1.0.1\" to have minor and patch version of 0",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := s.MetaWrite(c.ctx, c.subject, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
