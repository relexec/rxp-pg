package store_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	testutil "github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/object"
	readoption "github.com/relexec/rxp/object/read/option"
	"github.com/relexec/rxp/object/read/selector"
	writeoption "github.com/relexec/rxp/object/write/option"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/application"
	"github.com/relexec/rxp/testing/fixtures/book"
	"github.com/relexec/rxp/testing/fixtures/platform"
	"github.com/relexec/rxp/testing/fixtures/service"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestObjectRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	err = testutil.EnsureKind(ctx, s, platform.Kind)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, platform.FirstMeta())
	require.Nil(t, err)

	err = testutil.EnsureKind(ctx, s, application.Kind)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, application.FirstMeta())
	require.Nil(t, err)

	err = testutil.EnsureKind(ctx, s, service.Kind)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, service.FirstMeta())
	require.Nil(t, err)

	err = testutil.EnsureKind(ctx, s, book.Kind)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, book.FirstMeta())
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	domain := fixtures.Domain
	ns := fixtures.Namespace

	book1 := object.New(
		object.WithKindVersion(book.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
		object.WithName(testutil.RandomName()),
	)

	err = testutil.EnsureObject(ctx, s, book1)
	require.Nil(t, err)

	cases := []struct {
		name   string
		ctx    context.Context
		sel    selector.Selector
		opts   []readoption.Option
		exp    rxptypes.Object
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
			got, err := s.ObjectRead(c.ctx, c.sel, c.opts...)
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

func TestObjectWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	err = testutil.EnsureKind(ctx, s, book.Kind)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, book.FirstMeta())
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	domain := fixtures.Domain
	ns := fixtures.Namespace

	bookMissingUUID := object.New(
		object.WithKindVersion(book.FirstKindVersion()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
		object.WithName("book1"),
	)

	bookMissingName := object.New(
		object.WithKindVersion(book.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
	)

	bookMissingNamespace := object.New(
		object.WithKindVersion(book.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithName(testutil.RandomName()),
	)

	book1 := object.New(
		object.WithKindVersion(book.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
		object.WithName(testutil.RandomName()),
	)
	book1Name := book1.Name()
	bookDuplicateName := object.New(
		object.WithKindVersion(book.FirstKindVersion()),
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
		object.WithName(book1Name),
	)

	cases := []struct {
		name    string
		ctx     context.Context
		subject rxptypes.Object
		opts    []writeoption.Option
		exp     rxptypes.Object
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownObject,
			nil,
			nil,
			"missing identity",
		},
		{
			"missing uuid",
			ctx,
			bookMissingUUID,
			nil,
			nil,
			"invalid object: missing uuid",
		},
		{
			"missing name",
			ctx,
			bookMissingName,
			nil,
			nil,
			"invalid object: missing name",
		},
		{
			"namespace required",
			ctx,
			bookMissingNamespace,
			nil,
			nil,
			"invalid object: namespace required",
		},
		{
			"unknown kind version",
			ctx,
			fixtures.UnknownObject,
			nil,
			nil,
			"unknown kind version",
		},
		{
			"happy path",
			ctx,
			book1,
			nil,
			book1,
			"",
		},
		{
			"namespace-qualified name collision",
			ctx,
			bookDuplicateName,
			nil,
			nil,
			"conflict: \"book.testing.rxp\" already exists with name",
		},
		// Attempting to write the same object without specifying an expected
		// generation should result in a precondition failed.
		{
			"object already exists",
			ctx,
			book1,
			nil,
			book1,
			"not to exist",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := s.ObjectWrite(c.ctx, c.subject, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
