package driver

import (
	"context"
	"fmt"
	"time"

	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storerun "github.com/relexec/rxp-pg/internal/store/run"
	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/api/metrics"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/query"
	"github.com/relexec/rxp/run"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RunRead reads a single Run from persistent storage.
func (d *Driver) RunRead(
	ctx context.Context,
	target api.RunTarget,
	sel run.Selector,
) (*api.Run, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeRun),
		}
		if err != nil {
			attrs = append(attrs, metrics.AttributeErrCode(err))
		}
		metrics.InstrumentReadRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentReadDuration.Record(ctx, elapsed)
	}()

	err = d.runReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	uuid := sel.UUID()

	rec, err := d.runStore.ReadByUUID(ctx, uuid)
	if err != nil {
		return nil, err
	}
	rr := rec.Run.Request()
	rr.Target = target
	rec.Run.SetRequest(rr)
	return rec.Run, nil
}

// runReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Run.
func (d *Driver) runReadValidate(
	ctx context.Context,
	sel run.Selector,
) error {
	return sel.Validate()
}

// RunWrite persists a single supplied Run to backend storage, Note that
// on successful write, the newly-created or updated Run is returned.
func (d *Driver) RunWrite(
	ctx context.Context,
	run api.Run,
) (*api.Run, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	var targetKV api.KindVersionName

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeRun),
			metrics.AttributeKindVersion(targetKV),
		}
		if err != nil {
			attrs = append(attrs, metrics.AttributeErrCode(err))
		}
		metrics.InstrumentWriteRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentWriteDuration.Record(ctx, elapsed)
	}()

	err = d.runWriteValidate(ctx, run)
	if err != nil {
		return nil, err
	}

	req := run.Request()
	target := req.Target
	targetKV = target.KindVersionName
	targetSys := target.System
	targetDom := target.Domain
	caller := req.Caller

	if targetDom != nil {
		targetSys = targetDom.System
	}

	callerSys := caller.System
	callerDom := caller.Domain

	if callerDom != nil {
		callerSys = callerDom.System
	}

	// Default the target and caller system to the host system if it hasn't
	// been specified.
	if callerSys == nil {
		callerSys = &d.hostSystemRecord.System
		if callerDom != nil {
			callerDom.System = callerSys
		}
	}
	if targetSys == nil {
		targetSys = &d.hostSystemRecord.System
		if targetDom != nil {
			targetDom.System = targetSys
		}
	}

	callerSysRec, err := d.systemRecordFromSystem(ctx, callerSys)
	if err != nil {
		return nil, err
	}

	targetSysRec, err := d.systemRecordFromSystem(ctx, targetSys)
	if err != nil {
		return nil, err
	}

	var callerDomRec *storedomain.Record

	if callerDom != nil {
		callerDomRec, err = d.domainRecordFromDomain(
			ctx, *callerSysRec, callerDom,
		)
		if err != nil {
			return nil, err
		}
	}
	if targetDom != nil {
		_, err := d.domainRecordFromDomain(
			ctx, *targetSysRec, targetDom,
		)
		if err != nil {
			return nil, err
		}
	}

	targetKindRec, err := d.kindStore.ReadByName(
		ctx, *targetSysRec, targetKV.Kind(),
	)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrKindUnknown
		}
		return nil, err
	}

	_, err = d.kindversionStore.ReadByName(
		ctx, *targetSysRec, *targetKindRec, targetKV,
	)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrKindUnknown
		}
		return nil, err
	}
	targetRec, err := d.objectStore.ReadByUUIDAndGeneration(
		ctx, target.UUID, target.Generation,
	)
	if err != nil {
		return nil, err
	}
	targetRec.Object.System = &targetSysRec.System
	if targetDom != nil {
		targetRec.Object.Domain = targetDom
	}
	targetRec.Object.KindVersionName = targetKV

	var rootRec *storerun.Record
	var parentRec *storerun.Record

	rootUUID := run.Root()
	if rootUUID != "" && rootUUID != req.UUID {
		rootRec, err = d.runStore.ReadByUUID(ctx, rootUUID)
		if err != nil {
			return nil, fmt.Errorf(
				"failed reading root run by uuid: %w", err,
			)
		}
	}
	parentUUID := run.Parent()
	if parentUUID != "" {
		// short-circuit if we've already looked up the root...
		if parentUUID == rootUUID {
			parentRec = rootRec
		} else {
			parentRec, err = d.runStore.ReadByUUID(ctx, parentUUID)
			if err != nil {
				return nil, fmt.Errorf(
					"failed reading parent run by uuid: %w", err,
				)
			}
		}
	}

	return d.runStore.Write(
		ctx,
		*targetRec,
		*callerSysRec, callerDomRec,
		rootRec, parentRec, run,
	)
}

// runWriteValidate returns an error if the supplied run and write
// options are not valid for writing a single Run.
func (d *Driver) runWriteValidate(
	ctx context.Context,
	run api.Run,
) error {
	return run.Validate()
}

const (
	DefaultRunQueryLimit = 10
	MaxRunQueryLimit     = 100
)

// RunQuery queries zero or more Runs of a specified kind or kindversion
// from persistent storage.
func (d *Driver) RunQuery(
	ctx context.Context,
	expr query.Expression,
	opts ...query.Option,
) (*query.Result[*api.Run], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeRun),
		}
		if err != nil {
			attrs = append(attrs, metrics.AttributeErrCode(err))
		}
		metrics.InstrumentQueryRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentQueryDuration.Record(ctx, elapsed)
	}()

	qopts := query.NewOptions(opts...)
	err = d.runQueryValidate(ctx, expr, qopts)
	if err != nil {
		return nil, err
	}

	boundedOpts := d.runQueryBoundedOptions(ctx, qopts)

	recs, err := d.runStore.Query(
		ctx, expr, boundedOpts,
	)
	if err != nil {
		return nil, err
	}
	objs := make([]*api.Run, 0, len(recs))
	for _, rec := range recs {
		objs = append(objs, rec.Run)
	}
	resNewOpts := []query.ResultModifier[*api.Run]{
		query.ResultWithItems(objs),
		query.ResultWithOptions[*api.Run](boundedOpts),
	}
	if len(recs) == int(boundedOpts.Limit()) {
		resNewOpts = append(
			resNewOpts,
			query.ResultWithMarker[*api.Run](
				recs[len(recs)-1].Run.UUID(),
			),
		)
	}
	return query.NewResult[*api.Run](resNewOpts...), nil
}

// runQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) runQueryValidate(
	ctx context.Context,
	expr query.Expression,
	opts query.Options,
) error {
	return nil
}

// runQueryBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records queryed is
// less than the max page result.
func (d *Driver) runQueryBoundedOptions(
	ctx context.Context,
	opts query.Options,
) query.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultRunQueryLimit
	}
	limit = min(limit, MaxRunQueryLimit)
	return query.NewOptions(query.Limit(limit))
}
