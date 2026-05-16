package store_test

import (
	"context"
	"testing"

	"github.com/relexec/rxp-pg/internal/testutil"
	writeoption "github.com/relexec/rxp/system/write/option"
	"github.com/relexec/rxp/testing/fixtures"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/require"
)

func TestSystemWrite(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	cases := []struct {
		name    string
		ctx     context.Context
		subject rxptypes.System
		opts    []writeoption.Option
		expErr  string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			fixtures.UnknownSystem,
			nil,
			"missing identity",
		},
		{
			"duplicate system",
			ctx,
			fixtures.System,
			nil,
			"conflict: \"system\" already exists",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			err := s.SystemWrite(c.ctx, c.subject, c.opts...)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.Nil(err)
			}
		})
	}
}
