package store_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	readoption "github.com/relexec/rxp/read/option"
	"github.com/relexec/rxp/read/selector"
	"github.com/relexec/rxp/testing/fixtures"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestSystemRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name   string
		ctx    context.Context
		sel    rxptypes.Selector
		opts   []readoption.Option
		exp    rxptypes.System
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			selector.New(selector.WithUUID(fixtures.SystemUUID)),
			nil,
			nil,
			"missing identity",
		},
		{
			"unknown system",
			ctx,
			selector.New(selector.WithUUID(fixtures.UnknownSystemUUID)),
			nil,
			nil,
			"not found",
		},
		{
			"happy path",
			ctx,
			selector.New(selector.WithUUID(fixtures.SystemUUID)),
			nil,
			fixtures.System,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := s.SystemRead(c.ctx, c.sel, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
				delta, err := c.exp.Diff(got)
				require.Nil(err)
				require.False(delta.Different(), delta.Differences())
			}
		})
	}
}
