package driver_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/cmp/fieldpath"
	"github.com/relexec/rxp/domain"
	writeoption "github.com/relexec/rxp/domain/write/option"
	readoption "github.com/relexec/rxp/read/option"
	selector "github.com/relexec/rxp/read/selector/domain"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/types"
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
		sel    types.Selector
		opts   []readoption.Option
		exp    types.Domain
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			selector.New(
				selector.WithName(fixtures.InvalidDomainName),
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
			"unknown domain",
			ctx,
			selector.New(
				selector.WithName(fixtures.UnknownDomainName),
			),
			nil,
			nil,
			"not found",
		},
		{
			"invalid domain",
			ctx,
			selector.New(
				selector.WithName(fixtures.InvalidDomainName),
			),
			nil,
			nil,
			"invalid domain name: invalid characters",
		},
		{
			"happy path by uuid",
			ctx,
			selector.New(
				selector.WithUUID(fixtures.Domain.UUID()),
			),
			nil,
			fixtures.Domain,
			"",
		},
		{
			"happy path by name",
			ctx,
			selector.New(
				selector.WithName(fixtures.Domain.Name()),
			),
			nil,
			fixtures.Domain,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.DomainRead(c.ctx, c.sel, c.opts...)
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
		subject types.Domain
		opts    []writeoption.Option
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownDomain,
			nil,
			"missing identity",
		},
		{
			"invalid domain",
			ctx,
			fixtures.InvalidDomain,
			nil,
			"invalid domain name: invalid characters",
		},
		{
			"duplicate domain UUID",
			ctx,
			domain.New(
				domain.WithUUID(fixtures.Domain.UUID()),
				domain.WithName("othername"),
			),
			nil,
			"conflict: \"domain\" already exists",
		},
		{
			"duplicate domain name",
			ctx,
			domain.New(
				domain.WithUUID(uuid.NewString()),
				domain.WithName(fixtures.Domain.Name()),
			),
			nil,
			"conflict: \"domain\" already exists",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := rxp.DomainWrite(c.ctx, c.subject, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
