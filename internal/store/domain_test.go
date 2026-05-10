package store_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/cmp/fieldpath"
	readoption "github.com/relexec/rxp/domain/read/option"
	"github.com/relexec/rxp/domain/read/selector"
	writeoption "github.com/relexec/rxp/domain/write/option"
	"github.com/relexec/rxp/testing/fixtures"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestDomainRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	err = testutil.EnsureDomain(ctx, s, fixtures.Domain)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name   string
		ctx    context.Context
		sel    selector.Selector
		opts   []readoption.Option
		exp    rxptypes.Domain
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			selector.New(selector.WithName(fixtures.InvalidDomainName)),
			nil,
			nil,
			"missing identity",
		},
		{
			"unknown domain",
			ctx,
			selector.New(selector.WithName(fixtures.UnknownDomainName)),
			nil,
			nil,
			"not found",
		},
		{
			"invalid domain",
			ctx,
			selector.New(selector.WithName(fixtures.InvalidDomainName)),
			nil,
			nil,
			"invalid domain name: invalid characters",
		},
		{
			"happy path",
			ctx,
			selector.New(selector.WithName(fixtures.Domain.Name())),
			nil,
			fixtures.Domain,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := s.DomainRead(c.ctx, c.sel, c.opts...)
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
				)
			}
		})
	}
}

func TestDomainWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	err = testutil.EnsureDomain(ctx, s, fixtures.Domain)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name    string
		ctx     context.Context
		subject rxptypes.Domain
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
			"duplicate domain",
			ctx,
			fixtures.Domain,
			nil,
			"conflict: \"domain\" already exists",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := s.DomainWrite(c.ctx, c.subject, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
