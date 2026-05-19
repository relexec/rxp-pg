package driver_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	testutil "github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/object"
	readoption "github.com/relexec/rxp/object/read/option"
	"github.com/relexec/rxp/object/read/selector"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/application"
	"github.com/relexec/rxp/testing/fixtures/book"
	"github.com/relexec/rxp/testing/fixtures/platform"
	"github.com/relexec/rxp/testing/fixtures/service"
	"github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestObjectRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, platform.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, platform.FirstMeta())
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, application.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, application.FirstMeta())
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, service.Kind)
	require.Nil(t, err, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, service.FirstMeta())
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, book.Kind)
	require.Nil(t, err)

	err = testutil.MetaCreateIfNotExists(ctx, rxp, book.FirstMeta())
	require.Nil(t, err)

	domain := fixtures.Domain
	err = testutil.DomainCreateIfNotExists(ctx, rxp, domain)
	require.Nil(t, err)

	ns := fixtures.Namespace
	err = testutil.NamespaceCreateIfNotExists(ctx, rxp, ns)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	book1 := object.New(
		object.WithKindVersion(book.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
		object.WithName(testutil.RandomName()),
	)

	err = testutil.ObjectCreateIfNotExists(ctx, rxp, book1)
	require.Nil(t, err)

	cases := []struct {
		name   string
		ctx    context.Context
		sel    selector.Selector
		opts   []readoption.Option
		exp    types.Object
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			selector.New(
				selector.WithKindVersion(fixtures.UnknownKindVersion),
				selector.WithNamespace(book1.Namespace()),
				selector.WithUUID(book1.UUID()),
			),
			nil,
			nil,
			"missing identity",
		},
		{
			"unknown kind version",
			ctx,
			selector.New(
				selector.WithKindVersion(fixtures.UnknownKindVersion),
				selector.WithNamespace(book1.Namespace()),
				selector.WithUUID(book1.UUID()),
			),
			nil,
			nil,
			"unknown kind version",
		},
		{
			"invalid kind version",
			ctx,
			selector.New(
				selector.WithKindVersion(fixtures.InvalidKindVersion),
				selector.WithNamespace(book1.Namespace()),
				selector.WithUUID(book1.UUID()),
			),
			nil,
			nil,
			"invalid kind name: invalid characters",
		},
		{
			"kind version required in selector",
			ctx,
			selector.New(),
			nil,
			nil,
			"invalid selector: kindversion required",
		},
		{
			"missing domain with name",
			ctx,
			selector.New(
				selector.WithKindVersion(application.FirstKindVersion()),
				selector.WithName(testutil.RandomName()),
			),
			nil,
			nil,
			"invalid selector: domain required",
		},
		{
			"missing namespace with name",
			ctx,
			selector.New(
				selector.WithKindVersion(service.FirstKindVersion()),
				selector.WithName(testutil.RandomName()),
			),
			nil,
			nil,
			"invalid selector: namespace required",
		},
		{
			"mismatched kind",
			ctx,
			selector.New(
				selector.WithKindVersion(application.FirstKindVersion()),
				selector.WithDomain(book1.Domain()),
				selector.WithUUID(book1.UUID()),
			),
			nil,
			nil,
			"not found",
		},
		{
			"unknown generation",
			ctx,
			selector.New(
				selector.WithKindVersion(book.FirstKindVersion()),
				selector.WithNamespace(book1.Namespace()),
				selector.WithUUID(book1.UUID()),
				selector.WithGeneration(42),
			),
			nil,
			nil,
			"not found",
		},
		{
			"happy path by uuid",
			ctx,
			selector.New(
				selector.WithKindVersion(book.FirstKindVersion()),
				selector.WithNamespace(book1.Namespace()),
				selector.WithUUID(book1.UUID()),
			),
			nil,
			book1,
			"",
		},
		{
			"happy path by namespace-qualified name",
			ctx,
			selector.New(
				selector.WithKindVersion(book.FirstKindVersion()),
				selector.WithNamespace(book1.Namespace()),
				selector.WithName(book1.Name()),
			),
			nil,
			book1,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.ObjectRead(c.ctx, c.sel, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err, err)
				require.NotNil(got)
				require.Equal(c.exp.KindVersion(), got.KindVersion())
				if c.exp.Domain() != nil {
					require.NotNil(got.Domain())
					require.Equal(c.exp.Domain().Name(), got.Domain().Name())
				} else {
					require.Nil(got.Domain())
				}
				if c.exp.Namespace() != nil {
					require.NotNil(got.Namespace())
					require.Equal(c.exp.Namespace().Name(), got.Namespace().Name())
				} else {
					require.Nil(got.Namespace())
				}
				require.Equal(c.exp.Name(), got.Name())
				require.Equal(c.exp.UUID(), got.UUID())
				// TODO(jaypipes): finish coding Object.Diff
				//require.Equal(got, c.exp)
			}
		})
	}
}
