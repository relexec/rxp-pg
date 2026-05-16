package store_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	writeoption "github.com/relexec/rxp/namespace/write/option"
	"github.com/relexec/rxp/testing/fixtures"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestNamespaceWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	err = testutil.EnsureNamespace(ctx, s, fixtures.Namespace)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name    string
		ctx     context.Context
		subject rxptypes.Namespace
		opts    []writeoption.Option
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownNamespace,
			nil,
			"missing identity",
		},
		{
			"invalid namespace",
			ctx,
			fixtures.InvalidNamespace,
			nil,
			"invalid namespace name: invalid characters",
		},
		{
			"duplicate namespace",
			ctx,
			fixtures.Namespace,
			nil,
			"conflict: \"namespace\" already exists",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := s.NamespaceWrite(c.ctx, c.subject, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
