package driver_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/cmp/fieldpath"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/query/expression"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestDomainRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.DomainCreateIfNotExists(ctx, rxp, fixtures.Domain)
	require.Nil(t, err, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name   string
		ctx    context.Context
		sel    domain.Selector
		exp    *domain.Domain
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			domain.Select(domain.ByName(fixtures.InvalidDomainName)),
			nil,
			"missing identity",
		},
		{
			"uuid or name required",
			ctx,
			domain.Select(),
			nil,
			"uuid or name required",
		},
		{
			"unknown domain",
			ctx,
			domain.Select(domain.ByName(fixtures.UnknownDomainName)),
			nil,
			"not found",
		},
		{
			"invalid domain",
			ctx,
			domain.Select(domain.ByName(fixtures.InvalidDomainName)),
			nil,
			"invalid domain name: invalid characters",
		},
		{
			"happy path by uuid",
			ctx,
			domain.Select(domain.ByUUID(fixtures.Domain.UUID())),
			fixtures.Domain,
			"",
		},
		{
			"happy path by name",
			ctx,
			domain.Select(domain.ByName(fixtures.Domain.Name())),
			fixtures.Domain,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.DomainRead(c.ctx, c.sel)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
				delta, err := c.exp.Diff(got)
				require.Nil(err)
				require.False(
					delta.DifferentExcept(
						fieldpath.FromString("system"),
					),
					delta.Differences(),
				)
			}
		})
	}
}

func TestDomainWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.DomainCreateIfNotExists(ctx, rxp, fixtures.Domain)
	require.Nil(t, err, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name    string
		ctx     context.Context
		subject *domain.Domain
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownDomain,
			"missing identity",
		},
		{
			"missing uuid",
			ctx,
			domain.New(),
			"invalid domain: uuid required",
		},
		{
			"missing name",
			ctx,
			domain.New(domain.WithUUID(uuid.NewString())),
			"invalid domain: name required",
		},
		{
			"invalid domain",
			ctx,
			fixtures.InvalidDomain,
			"invalid domain name: invalid characters",
		},
		{
			"duplicate domain UUID",
			ctx,
			domain.New(
				domain.WithUUID(fixtures.Domain.UUID()),
				domain.WithName("othername"),
			),
			"conflict: \"domain\" already exists",
		},
		{
			"duplicate domain name",
			ctx,
			domain.New(
				domain.WithUUID(uuid.NewString()),
				domain.WithName(fixtures.Domain.Name()),
			),
			"conflict: \"domain\" already exists",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := rxp.DomainWrite(c.ctx, c.subject)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}

func TestDomainQuery(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.DomainCreateIfNotExists(ctx, rxp, fixtures.Domain)
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
			expression.UUIDEqual(fixtures.DomainUUID),
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
			expression.GenerationEqual(0),
			nil,
			0,
			nil,
			query.Options{},
			"",
			"unsupported predicate expression.GenerationPredicate",
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
			"no results when looking up non-existing domain UUID",
			ctx,
			expression.UUIDEqual(fixtures.UnknownDomainUUID),
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
			"no results when looking up non-existing domain name",
			ctx,
			expression.DomainNameEqual(fixtures.UnknownDomainName),
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
			"no results when looking up domains by non-existing system",
			ctx,
			expression.SystemEqual(fixtures.UnknownSystem),
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
			"no results when looking up domains by non-existing system UUID",
			ctx,
			expression.SystemUUIDEqual(fixtures.UnknownSystemUUID),
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
			"query domains by name, expect one",
			ctx,
			expression.DomainNameEqual(fixtures.DomainName),
			nil,
			1,
			[]string{
				fixtures.DomainUUID,
			},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query domains by UUID, expect one",
			ctx,
			expression.UUIDEqual(fixtures.DomainUUID),
			nil,
			1,
			[]string{
				fixtures.DomainUUID,
			},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query domains by UUID in, expect one",
			ctx,
			expression.UUIDIn(fixtures.DomainUUID, fixtures.UnknownDomainUUID),
			nil,
			1,
			[]string{
				fixtures.DomainUUID,
			},
			query.NewOptions(
				query.Limit(10), // 10 is default when not specified
			),
			"",
			"",
		},
		{
			"query domains by domain UUID, expect one",
			ctx,
			expression.DomainUUIDEqual(fixtures.DomainUUID),
			nil,
			1,
			[]string{
				fixtures.DomainUUID,
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

			got, err := rxp.DomainQuery(c.ctx, c.expr, c.opts...)
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
				gotUUIDs := lo.Map(gotItems, func(d *domain.Domain, _ int) string {
					return d.UUID()
				})
				gotUUIDs = lo.Uniq(gotUUIDs)
				require.Equal(c.expOnlyUUIDs, gotUUIDs)
				for _, item := range gotItems {
					require.NotNil(item.System())
				}
			}
		})
	}
}
