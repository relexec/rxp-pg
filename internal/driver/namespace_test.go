package driver_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/namespace"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/samber/lo"
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

func TestNamespaceQuery(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.NamespaceCreateIfNotExists(ctx, rxp, fixtures.Namespace)
	require.Nil(t, err, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name         string
		ctx          context.Context
		expr         expression.Expression
		opts         []query.Option
		expNumItems  int
		expOnlyUUIDs []string
		expOptions   query.Options
		expMarker    string
		expErr       string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			expression.UUIDEqual(fixtures.NamespaceUUID),
			nil,
			0,
			nil,
			query.Options{},
			"",
			"missing identity",
		},
		{
			"unsupported predicate",
			ctx,
			expression.DomainNameEqual(fixtures.DomainName),
			nil,
			0,
			nil,
			query.Options{},
			"",
			"unsupported predicate expression.DomainNamePredicate",
		},
		{
			"expression required",
			ctx,
			nil,
			nil,
			0,
			nil,
			query.Options{},
			"",
			"expression required",
		},
		{
			"unsupported expression",
			ctx,
			expression.Or(
				expression.DomainNameEqual(fixtures.DomainName),
				expression.DomainNameEqual(fixtures.UnknownDomainName),
			),
			nil,
			0,
			nil,
			query.Options{},
			"",
			"unsupported expression expression.OrExpression",
		},
		{
			"no results when looking up non-existing namespace UUID",
			ctx,
			expression.UUIDEqual(fixtures.UnknownNamespaceUUID),
			nil,
			0,
			[]string{},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query namespaces by UUID, expect one",
			ctx,
			expression.UUIDEqual(fixtures.NamespaceUUID),
			nil,
			1,
			[]string{
				fixtures.NamespaceUUID,
			},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query namespaces by UUID in, expect one",
			ctx,
			expression.UUIDIn(fixtures.NamespaceUUID, fixtures.UnknownNamespaceUUID),
			nil,
			1,
			[]string{
				fixtures.NamespaceUUID,
			},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)

			got, err := rxp.NamespaceQuery(c.ctx, c.expr, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err, err)
				require.NotNil(got)
				gotItems := got.Items()
				gotOptions := got.Options()
				gotMarker := got.Marker()
				require.Equal(c.expOptions, gotOptions)
				require.Equal(c.expMarker, gotMarker)
				require.Len(gotItems, c.expNumItems)
				gotUUIDs := lo.Map(gotItems, func(n *namespace.Namespace, _ int) string {
					return n.UUID()
				})
				gotUUIDs = lo.Uniq(gotUUIDs)
				require.Equal(c.expOnlyUUIDs, gotUUIDs)
				for _, item := range gotItems {
					require.NotNil(item.Domain())
					require.NotNil(item.Domain().System())
				}
			}
		})
	}
}
