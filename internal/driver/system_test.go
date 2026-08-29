package driver_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp-testing/fixtures"
	rxpdomain "github.com/relexec/rxp/api/domain"
	rxpquery "github.com/relexec/rxp/api/query"
	rxpsystem "github.com/relexec/rxp/api/system"
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
		sel    rxpsystem.Selector
		exp    *rxpsystem.System
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			rxpsystem.Select(rxpsystem.ByUUID(fixtures.SystemUUID)),
			nil,
			"missing identity",
		},
		{
			"uuid required",
			ctx,
			rxpsystem.Selector{},
			nil,
			"uuid required",
		},
		{
			"unknown system",
			ctx,
			rxpsystem.Select(rxpsystem.ByUUID(fixtures.UnknownSystemUUID)),
			nil,
			"not found",
		},
		{
			"happy path",
			ctx,
			rxpsystem.Select(rxpsystem.ByUUID(fixtures.SystemUUID)),
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
				delta, err := rxpsystem.Diff(*c.exp, got)
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
		subject *rxpsystem.System
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
		expr         rxpquery.Expression
		opts         []rxpquery.Option
		expNumItems  int
		expOnlyUUIDs []string
		expOptions   rxpquery.Options
		expMarker    string
		expErr       string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			rxpsystem.UUIDEqual(fixtures.SystemUUID),
			nil,
			0,
			nil,
			rxpquery.Options{},
			"",
			"missing identity",
		},
		{
			"unsupported predicate",
			ctx,
			rxpdomain.NameEqual(fixtures.DomainName),
			nil,
			0,
			nil,
			rxpquery.Options{},
			"",
			"unsupported predicate",
		},
		{
			"expression required",
			ctx,
			nil,
			nil,
			0,
			nil,
			rxpquery.Options{},
			"",
			"expression required",
		},
		{
			"unsupported expression",
			ctx,
			rxpquery.Or(
				rxpdomain.NameEqual(fixtures.DomainName),
				rxpdomain.NameEqual(fixtures.UnknownDomainName),
			),
			nil,
			0,
			nil,
			rxpquery.Options{},
			"",
			"unsupported expression",
		},
		{
			"no results when looking up non-existing system UUID",
			ctx,
			rxpsystem.UUIDEqual(fixtures.UnknownSystemUUID),
			nil,
			0,
			[]string{},
			rxpquery.NewOptions(
				rxpquery.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query systems by UUID, expect one",
			ctx,
			rxpsystem.UUIDEqual(fixtures.SystemUUID),
			nil,
			1,
			[]string{
				fixtures.SystemUUID,
			},
			rxpquery.NewOptions(
				rxpquery.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query systems by UUID in, expect one",
			ctx,
			rxpsystem.UUIDIn(fixtures.SystemUUID, fixtures.UnknownSystemUUID),
			nil,
			1,
			[]string{
				fixtures.SystemUUID,
			},
			rxpquery.NewOptions(
				rxpquery.Limit(10), // 10 is default when not specified
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
				gotUUIDs := lo.Map(gotItems, func(s *rxpsystem.System, _ int) string {
					return s.UUID
				})
				gotUUIDs = lo.Uniq(gotUUIDs)
				require.Equal(c.expOnlyUUIDs, gotUUIDs)
			}
		})
	}
}
