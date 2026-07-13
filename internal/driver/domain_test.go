package driver_test

import (
	"context"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/delta/fieldpath"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/system"
	"github.com/relexec/rxp-testing/fixtures"
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
						fieldpath.FromString("root"),
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
		{
			"parent domain does not exist",
			ctx,
			domain.New(
				domain.WithUUID(uuid.NewString()),
				domain.WithName("parent.not.exist"),
				domain.WithRoot(fixtures.UnknownDomain),
				domain.WithParent(fixtures.UnknownDomain),
			),
			"invalid domain: parent not found",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := rxp.DomainWrite(c.ctx, *c.subject)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}

func TestDomainTree(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	treeDoms := []*domain.Domain{
		fixtures.DomainTree_Root,
		fixtures.DomainTree_Group1,
		fixtures.DomainTree_Group2,
		fixtures.DomainTree_Group1Leaf1,
		fixtures.DomainTree_Group1Leaf2,
		fixtures.DomainTree_Group2Leaf1,
		fixtures.DomainTree_Group2Leaf2,
	}
	treeDomUUIDs := lo.Map(treeDoms, func(d *domain.Domain, _ int) string {
		return d.UUID()
	})
	sort.Strings(treeDomUUIDs)

	for _, dom := range treeDoms {
		err = testutil.DomainCreateIfNotExists(ctx, rxp, dom)
		require.Nil(t, err, err)
	}

	got, err := rxp.DomainQuery(
		ctx, domain.UUIDEqual(
			fixtures.DomainTree_RootUUID,
		),
	)
	require.Nil(t, err)
	items := got.Items()
	require.Len(t, items, 1)
	require.Nil(t, items[0].Parent())

	// Grabbing all domains in the domain tree should yield all domains in the
	// tree.
	got, err = rxp.DomainQuery(
		ctx, domain.RootUUIDEqual(
			fixtures.DomainTree_RootUUID,
		),
	)
	require.Nil(t, err)
	items = got.Items()
	require.Len(t, items, len(treeDoms))

	gotDomUUIDs := lo.Map(items, func(d *domain.Domain, _ int) string {
		return d.UUID()
	})
	sort.Strings(gotDomUUIDs)

	require.Equal(t, treeDomUUIDs, gotDomUUIDs)

	// Querying by root name should also yield all domaisn in the tree.
	got, err = rxp.DomainQuery(
		ctx, domain.RootNameEqual(
			fixtures.DomainTree_RootName,
		),
	)
	require.Nil(t, err)
	items = got.Items()
	require.Len(t, items, len(treeDoms))

	gotDomUUIDs = lo.Map(items, func(d *domain.Domain, _ int) string {
		return d.UUID()
	})
	sort.Strings(gotDomUUIDs)

	require.Equal(t, treeDomUUIDs, gotDomUUIDs)

	// Grabbing domains having a parent equal to the root should yield all
	// domains in the tree.
	got, err = rxp.DomainQuery(
		ctx, domain.ParentUUIDEqual(
			fixtures.DomainTree_RootUUID,
		),
	)
	require.Nil(t, err)
	items = got.Items()
	require.Len(t, items, len(treeDoms))

	gotDomUUIDs = lo.Map(items, func(d *domain.Domain, _ int) string {
		return d.UUID()
	})
	sort.Strings(gotDomUUIDs)

	require.Equal(t, treeDomUUIDs, gotDomUUIDs)

	// Grabbing domains within a subdomain should yield only that subdomain and
	// its child domains.
	groupDoms := []*domain.Domain{
		fixtures.DomainTree_Group1,
		fixtures.DomainTree_Group1Leaf1,
		fixtures.DomainTree_Group1Leaf2,
	}
	got, err = rxp.DomainQuery(
		ctx, domain.ParentUUIDEqual(
			fixtures.DomainTree_Group1UUID,
		),
	)
	require.Nil(t, err)
	items = got.Items()
	require.Len(t, items, len(groupDoms))

	groupDomUUIDs := lo.Map(groupDoms, func(d *domain.Domain, _ int) string {
		return d.UUID()
	})
	sort.Strings(groupDomUUIDs)
	gotDomUUIDs = lo.Map(items, func(d *domain.Domain, _ int) string {
		return d.UUID()
	})
	sort.Strings(gotDomUUIDs)

	require.Equal(t, groupDomUUIDs, gotDomUUIDs)

	// Grabbing domains parented by a leaf domain should yield only the leaf
	// domain.
	got, err = rxp.DomainQuery(
		ctx, domain.ParentUUIDEqual(
			fixtures.DomainTree_Group1Leaf1UUID,
		),
	)
	require.Nil(t, err)
	items = got.Items()
	require.Len(t, items, 1)

	require.Equal(t, fixtures.DomainTree_Group1Leaf1UUID, items[0].UUID())
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
			domain.UUIDEqual(fixtures.DomainUUID),
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
			object.GenerationEqual(0),
			nil,
			0,
			nil,
			query.Options{},
			"",
			"unsupported predicate object.GenerationPredicate",
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
				domain.NameEqual(fixtures.DomainName),
				domain.NameEqual(fixtures.UnknownDomainName),
			),
			nil,
			0,
			nil,
			query.Options{},
			"",
			"unsupported expression query.OrExpression",
		},
		{
			"no results when looking up non-existing domain UUID",
			ctx,
			domain.UUIDEqual(fixtures.UnknownDomainUUID),
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
			domain.NameEqual(fixtures.UnknownDomainName),
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
			system.Equal(fixtures.UnknownSystem),
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
			system.UUIDEqual(fixtures.UnknownSystemUUID),
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
			domain.NameEqual(fixtures.DomainName),
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
			domain.UUIDEqual(fixtures.DomainUUID),
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
			domain.UUIDIn(fixtures.DomainUUID, fixtures.UnknownDomainUUID),
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
			domain.UUIDEqual(fixtures.DomainUUID),
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
