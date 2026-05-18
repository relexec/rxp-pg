package driver_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/namespace"
	writeoption "github.com/relexec/rxp/namespace/write/option"
	readoption "github.com/relexec/rxp/read/option"
	selector "github.com/relexec/rxp/read/selector/namespace"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/types"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestNamespaceRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.NamespaceCreateIfNotExists(ctx, rxp, fixtures.Namespace)
	require.Nil(t, err, err)

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
			"uuid or name required",
			ctx,
			selector.New(),
			nil,
			nil,
			"uuid or name required",
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
			got, err := rxp.NamespaceRead(c.ctx, c.sel, c.opts...)
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

func TestNamespaceWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.NamespaceCreateIfNotExists(ctx, rxp, fixtures.Namespace)
	require.Nil(t, err, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name    string
		ctx     context.Context
		subject types.Namespace
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
			"missing uuid",
			ctx,
			namespace.New(
				namespace.WithDomain(fixtures.Domain),
			),
			nil,
			"invalid namespace: uuid required",
		},
		{
			"missing name",
			ctx,
			namespace.New(
				namespace.WithDomain(fixtures.Domain),
				namespace.WithUUID(uuid.NewString()),
			),
			nil,
			"invalid namespace: name required",
		},
		{
			"missing domain",
			ctx,
			namespace.New(
				namespace.WithUUID(uuid.NewString()),
				namespace.WithName(types.NamespaceName("mynamespace")),
			),
			nil,
			"invalid namespace: domain required",
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
			err := rxp.NamespaceWrite(c.ctx, c.subject, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
