package driver_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/namespace"
	"github.com/relexec/rxp/testing/fixtures"
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
		sel    namespace.Selector
		exp    *namespace.Namespace
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			namespace.Select(
				namespace.ByDomain(fixtures.Domain),
				namespace.ByName(fixtures.NamespaceName),
			),
			nil,
			"missing identity",
		},
		{
			"uuid or name required",
			ctx,
			namespace.Selector{},
			nil,
			"uuid or name required",
		},
		{
			"domain required",
			ctx,
			namespace.Select(
				namespace.ByName(fixtures.UnknownNamespaceName),
			),
			nil,
			"domain required",
		},
		{
			"unknown namespace",
			ctx,
			namespace.Select(
				namespace.ByDomain(fixtures.Domain),
				namespace.ByName(fixtures.UnknownNamespaceName),
			),
			nil,
			"not found",
		},
		{
			"invalid namespace",
			ctx,
			namespace.Select(
				namespace.ByDomain(fixtures.Domain),
				namespace.ByName(fixtures.InvalidNamespaceName),
			),
			nil,
			"invalid namespace name: invalid characters",
		},
		{
			"happy path",
			ctx,
			namespace.Select(
				namespace.ByDomain(fixtures.Domain),
				namespace.ByName(fixtures.NamespaceName),
			),
			fixtures.Namespace,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.NamespaceRead(c.ctx, c.sel)
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
		subject *namespace.Namespace
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownNamespace,
			"missing identity",
		},
		{
			"missing uuid",
			ctx,
			namespace.New(
				namespace.WithDomain(fixtures.Domain),
			),
			"invalid namespace: uuid required",
		},
		{
			"missing name",
			ctx,
			namespace.New(
				namespace.WithDomain(fixtures.Domain),
				namespace.WithUUID(uuid.NewString()),
			),
			"invalid namespace: name required",
		},
		{
			"missing domain",
			ctx,
			namespace.New(
				namespace.WithUUID(uuid.NewString()),
				namespace.WithName(api.NamespaceName("mynamespace")),
			),
			"invalid namespace: domain required",
		},
		{
			"invalid namespace",
			ctx,
			fixtures.InvalidNamespace,
			"invalid namespace name: invalid characters",
		},
		{
			"duplicate namespace",
			ctx,
			fixtures.Namespace,
			"conflict: \"namespace\" already exists",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := rxp.NamespaceWrite(c.ctx, c.subject)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
