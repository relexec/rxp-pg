package store_test

import (
	"testing"

	"github.com/google/uuid"
	testutil "github.com/relexec/rxp-pg/internal/testutil"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/object/read/selector"
	"github.com/relexec/rxp/testing/fixtures"
	"github.com/relexec/rxp/testing/fixtures/book"
	metricstesting "github.com/relexec/rxp/testing/metrics"
	rxptypes "github.com/relexec/rxp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetrics(t *testing.T) {
	ctx := testutil.Context(testutil.UserIdentity)
	s, err := testutil.Store(ctx)
	require.Nil(t, err)

	err = testutil.EnsureMeta(ctx, s, book.LatestMeta())
	require.Nil(t, err)

	m := s.Metrics()
	mr := m.Reader()

	domain := fixtures.Domain
	err = testutil.EnsureDomain(ctx, s, domain)
	require.Nil(t, err)

	ns := fixtures.Namespace
	err = testutil.EnsureNamespace(ctx, s, ns)
	require.Nil(t, err)

	booker := func(name string) rxptypes.Object {
		return book.New(
			object.WithUUID(uuid.NewString()),
			object.WithDomain(domain),
			object.WithNamespace(ns),
			object.WithName(name),
		)
	}
	book1 := booker("book1")
	book2 := booker("book2")

	before := &metricdata.ResourceMetrics{}
	mr.Collect(ctx, before)

	writeBefore := int64(0)
	ok := metricstesting.InResourceMetrics(
		before, metrics.InstrumentNameWriteRequest,
	)
	if ok {
		writeBefore, err = metricstesting.SumResourceMetrics(
			before, metrics.InstrumentNameWriteRequest,
		)
		require.Nil(t, err)
	}

	readBefore := int64(0)
	ok = metricstesting.InResourceMetrics(
		before, metrics.InstrumentNameReadRequest,
	)
	if ok {
		readBefore, err = metricstesting.SumResourceMetrics(
			before, metrics.InstrumentNameReadRequest,
		)
		require.Nil(t, err)
	}

	numWrite := int64(0)
	numRead := int64(0)

	// We call the Store's Read and Write methods a few times with some
	// expected errors thrown in for good measure and verify that the Store's
	// metrics handler collects the appropriate metrics.
	err = s.ObjectWrite(ctx, book2)
	require.Nil(t, err)
	numWrite++

	err = s.ObjectWrite(ctx, book1)
	require.Nil(t, err)
	numWrite++

	_, err = s.ObjectRead(
		ctx,
		selector.New(
			selector.WithKindVersion(book1.KindVersion()),
			selector.WithUUID(book1.UUID()),
		),
	)
	require.Nil(t, err)
	numRead++

	after := &metricdata.ResourceMetrics{}
	mr.Collect(ctx, after)

	writeAfter, err := metricstesting.SumResourceMetrics(
		after, metrics.InstrumentNameWriteRequest,
	)
	require.Nil(t, err)

	expWriteAfter := writeBefore + numWrite
	assert.Equal(t, expWriteAfter, writeAfter)

	readAfter, err := metricstesting.SumResourceMetrics(
		after, metrics.InstrumentNameReadRequest,
	)
	require.Nil(t, err)

	expReadAfter := readBefore + numRead
	assert.Equal(t, expReadAfter, readAfter)

	ok = metricstesting.InResourceMetrics(
		after, metrics.InstrumentNameReadDuration,
	)
	assert.True(t, ok)

	ok = metricstesting.InResourceMetrics(
		after, metrics.InstrumentNameWriteDuration,
	)
	assert.True(t, ok)
}
