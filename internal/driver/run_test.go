package driver_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	testutil "github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp-testing/fixtures"
	"github.com/relexec/rxp-testing/fixtures/runnable"
	apicore "github.com/relexec/rxp/api/core"
	apiobject "github.com/relexec/rxp/api/object"
	apirun "github.com/relexec/rxp/api/run"
	"github.com/stretchr/testify/require"
)

func TestRunRead(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	rxp, err := testutil.Driver(ctx)
	require.Nil(t, err)

	err = testutil.KindCreateIfNotExists(ctx, rxp, runnable.Kind)
	require.Nil(t, err, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, runnable.KindVersion_V1_0_0)
	require.Nil(t, err)

	err = testutil.KindVersionCreateIfNotExists(ctx, rxp, runnable.KindVersion_V1_0_1)
	require.Nil(t, err)

	dom := fixtures.Domain
	err = testutil.DomainCreateIfNotExists(ctx, rxp, dom)
	require.Nil(t, err)

	ctxMissingIdent := context.TODO()

	runnable1 := &apiobject.Object{
		KindVersionName: runnable.KindVersion_V1_0_0.Name(),
		UUID:            uuid.NewString(),
		Domain:          &dom,
		Name:            testutil.RandomName(),
	}

	err = testutil.ObjectCreateIfNotExists(ctx, rxp, runnable1)
	require.Nil(t, err, err)

	run1Target := apirun.Target{
		KindVersionName: runnable.KindVersion_V1_0_0.Name(),
		UUID:            runnable1.UUID,
		Generation:      1,
	}

	run1UUID := uuid.NewString()
	run1Caller := apicore.Caller{
		Identity: testutil.UserIdentity,
	}
	run1 := &apirun.Run{}
	run1.SetRequest(
		apirun.Request{
			Target: run1Target,
			Caller: run1Caller,
			UUID:   run1UUID,
			On:     time.Now().UTC(),
		},
	)

	err = testutil.RunCreateIfNotExists(ctx, rxp, run1)
	require.Nil(t, err, err)

	cases := []struct {
		name   string
		ctx    context.Context
		target apirun.Target
		sel    apirun.Selector
		exp    *apirun.Run
		expErr string
	}{
		{
			"missing identity",
			ctxMissingIdent,
			run1Target,
			apirun.Select(apirun.ByUUID(run1.UUID())),
			nil,
			"missing identity",
		},
		{
			"unknown uuid",
			ctx,
			run1Target,
			apirun.Select(apirun.ByUUID(uuid.NewString())),
			nil,
			"not found",
		},
		{
			"uuid required in selector",
			ctx,
			run1Target,
			apirun.Select(),
			nil,
			"invalid selector: uuid required",
		},
		{
			"happy path by uuid",
			ctx,
			run1Target,
			apirun.Select(apirun.ByUUID(run1.Request().UUID)),
			run1,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require := require.New(t)
			got, err := rxp.RunRead(c.ctx, c.target, c.sel)
			if c.expErr != "" {
				require.ErrorContains(err, c.expErr)
			} else {
				require.NotNil(got)
				target := got.Request().Target
				require.Nil(err, err)
				require.NotNil(got)
				require.Equal(
					c.target.KindVersionName,
					target.KindVersionName,
				)
				require.Equal(c.exp.Request().UUID, got.Request().UUID)
				// TODO(jaypipes): finish coding Object.Diff
				//require.Equal(got, c.exp)
			}
		})
	}
}
