package store_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	readoption "github.com/relexec/rxp/namespace/read/option"
	"github.com/relexec/rxp/namespace/read/selector"
	"github.com/relexec/rxp/testing/fixtures"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestNamespaceRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	err = testutil.EnsureNamespace(ctx, s, fixtures.Namespace)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name   string
		ctx    context.Context
		sel    selector.Selector
		opts   []readoption.Option
		exp    rxptypes.Namespace
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			selector.New(
				selector.WithDomain(fixtures.Domain),
				selector.WithName(fixtures.NamespaceName),
			),
			nil,
			nil,
			"missing identity",
		},
		{
			"unknown namespace",
			ctx,
			selector.New(
				selector.WithDomain(fixtures.Domain),
				selector.WithName(fixtures.UnknownNamespaceName),
			),
			nil,
			nil,
			"not found",
		},
		{
			"invalid namespace",
			ctx,
			selector.New(
				selector.WithDomain(fixtures.Domain),
				selector.WithName(fixtures.InvalidNamespaceName),
			),
			nil,
			nil,
			"invalid namespace name: invalid characters",
		},
		{
			"happy path",
			ctx,
			selector.New(
				selector.WithDomain(fixtures.Domain),
				selector.WithName(fixtures.NamespaceName),
			),
			nil,
			fixtures.Namespace,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := s.NamespaceRead(c.ctx, c.sel, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
				delta, err := c.exp.Diff(got)
				require.Nil(err)
				require.False(delta.Different(), delta.Differences())
			}
		})
	}
}
