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
	"github.com/relexec/rxp/testing/fixtures/book"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestObjectRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, book.LatestMeta())
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	domain := fixtures.Domain
	ns := fixtures.Namespace

	booker := func(name string) rxptypes.Object {
		return book.New(
			object.WithUUID(uuid.NewString()),
			object.WithDomain(domain),
			object.WithNamespace(ns),
			object.WithName(name),
		)
	}
	book1 := booker("book1")

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
				selector.WithUUID(book1.UUID()),
			),
			nil,
			nil,
			"invalid kind: invalid characters",
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
			"unknown generation",
			ctx,
			selector.New(
				selector.WithKindVersion(book.LatestKindVersion()),
				selector.WithUUID(book1.UUID()),
				selector.WithGeneration(42),
			),
			nil,
			nil,
			"not found",
		},
		{
			"happy path",
			ctx,
			selector.New(
				selector.WithKindVersion(book.LatestKindVersion()),
				selector.WithUUID(book1.UUID()),
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
				require.Nil(err)
				require.NotNil(got)
				require.Equal(c.exp.KindVersion(), got.KindVersion())
				require.Equal(c.exp.UUID(), got.UUID())
				require.Equal(c.exp.Name(), got.Name())
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

	err = testutil.EnsureMeta(ctx, s, book.LatestMeta())
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	domain := fixtures.Domain
	ns := fixtures.Namespace

	bookMissingUUID := book.New(
		object.WithDomain(domain),
		object.WithNamespace(ns),
		object.WithName("book1"),
	)

	bookMissingName := book.New(
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
	)

	book1 := book.New(
		object.WithUUID(uuid.NewString()),
		object.WithDomain(domain),
		object.WithNamespace(ns),
		object.WithName("book1"),
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
			"unknown kind version",
			ctx,
			fixtures.UnknownObject,
			nil,
			nil,
			"unknown kind version",
		},
		{
			"no write opts",
			ctx,
			book1,
			nil,
			book1,
			"",
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
