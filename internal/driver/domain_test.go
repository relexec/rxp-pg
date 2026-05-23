package driver_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/cmp/fieldpath"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/testing/fixtures"
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
