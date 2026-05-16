package store_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	writeoption "github.com/relexec/rxp/meta/write/option"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/author"
	"github.com/relexec/rxp/testing/fixtures/book"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestMetaWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	// NOTE(jaypipes): We ensure the author Kind here but not any Metas
	// (KindVersions) for it. This allows us to properly test the precondition
	// failed for minor/patch version number of 0.
	err = testutil.EnsureKind(ctx, s, author.Kind)
	require.Nil(t, err)

	err = testutil.EnsureKind(ctx, s, book.Kind)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, book.FirstMeta())
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
			err := s.MetaWrite(c.ctx, c.subject, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
