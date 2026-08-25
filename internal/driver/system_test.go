package driver_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp-testing/fixtures"
	apidomain "github.com/relexec/rxp/api/domain"
	apisystem "github.com/relexec/rxp/api/system"
	"github.com/relexec/rxp/query"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestSystemRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name   string
		ctx    context.Context
		sel    apisystem.Selector
		exp    *apisystem.System
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			apisystem.Select(apisystem.ByUUID(fixtures.SystemUUID)),
			nil,
			"missing identity",
		},
		{
			"uuid required",
			ctx,
			apisystem.Selector{},
			nil,
			"uuid required",
		},
		{
			"unknown system",
			ctx,
			apisystem.Select(apisystem.ByUUID(fixtures.UnknownSystemUUID)),
			nil,
			"not found",
		},
		{
			"happy path",
			ctx,
			apisystem.Select(apisystem.ByUUID(fixtures.SystemUUID)),
			&fixtures.System,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.SystemRead(c.ctx, c.sel)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
				delta, err := apisystem.Diff(*c.exp, got)
				require.Nil(err)
				require.False(delta.Different(), delta.Differences())
			}
		})
	}
}

func TestSystemWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name    string
		ctx     context.Context
		subject *apisystem.System
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			&fixtures.UnknownSystem,
			"missing identity",
		},
		{
			"duplicate system",
			ctx,
			&fixtures.System,
			"conflict: \"system\" already exists",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := rxp.SystemWrite(c.ctx, *c.subject)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}

func TestSystemQuery(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name         string
		ctx          context.Context
		expr         query.Expression
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
			apisystem.UUIDEqual(fixtures.SystemUUID),
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
			apidomain.NameEqual(fixtures.DomainName),
			nil,
			0,
			nil,
			query.Options{},
			"",
			"unsupported predicate apidomain.NamePredicate",
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
			query.Or(
				apidomain.NameEqual(fixtures.DomainName),
				apidomain.NameEqual(fixtures.UnknownDomainName),
			),
			nil,
			0,
			nil,
			query.Options{},
			"",
			"unsupported expression query.OrExpression",
		},
		{
			"no results when looking up non-existing system UUID",
			ctx,
			apisystem.UUIDEqual(fixtures.UnknownSystemUUID),
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
			"query systems by UUID, expect one",
			ctx,
			apisystem.UUIDEqual(fixtures.SystemUUID),
			nil,
			1,
			[]string{
				fixtures.SystemUUID,
			},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query systems by UUID in, expect one",
			ctx,
			apisystem.UUIDIn(fixtures.SystemUUID, fixtures.UnknownSystemUUID),
			nil,
			1,
			[]string{
				fixtures.SystemUUID,
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

			got, err := rxp.SystemQuery(c.ctx, c.expr, c.opts...)
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
				gotUUIDs := lo.Map(gotItems, func(s *apisystem.System, _ int) string {
					return s.UUID
				})
				gotUUIDs = lo.Uniq(gotUUIDs)
				require.Equal(c.expOnlyUUIDs, gotUUIDs)
			}
		})
	}
}
