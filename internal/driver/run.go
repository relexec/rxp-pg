package driver

import (
	"context"
	"fmt"
	"time"

	apicore "github.com/relexec/rxp/api/core"
	apidomain "github.com/relexec/rxp/api/domain"
	apierrors "github.com/relexec/rxp/api/errors"
	apikindversion "github.com/relexec/rxp/api/kindversion"
	apimetrics "github.com/relexec/rxp/api/metrics"
	apiquery "github.com/relexec/rxp/api/query"
	apirun "github.com/relexec/rxp/api/run"
	apisystem "github.com/relexec/rxp/api/system"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RunRead reads a single Run from persistent storage.
func (d *Driver) RunRead(
	ctx context.Context,
	target apirun.Target,
	sel apirun.Selector,
) (*apirun.Run, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeRun),
		}
		if err != nil {
			attrs = append(attrs, apimetrics.AttributeErrCode(err))
		}
		apimetrics.InstrumentReadRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		apimetrics.InstrumentReadDuration.Record(ctx, elapsed)
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
	rr := rec.Request()
	rr.Target = target
	rec.SetRequest(rr)
	return rec, nil
}

// runReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Run.
func (d *Driver) runReadValidate(
	ctx context.Context,
	sel apirun.Selector,
) error {
	return sel.Validate()
}

// RunWrite persists a single supplied Run to backend storage, Note that
// on successful write, the newly-created or updated Run is returned.
func (d *Driver) RunWrite(
	ctx context.Context,
	run apirun.Run,
) (*apirun.Run, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	var targetKV apikindversion.Name

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeRun),
			apimetrics.AttributeKindVersion(targetKV),
		}
		if err != nil {
			attrs = append(attrs, apimetrics.AttributeErrCode(err))
		}
		apimetrics.InstrumentWriteRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		apimetrics.InstrumentWriteDuration.Record(ctx, elapsed)
	}()

	err = d.runWriteValidate(ctx, run)
	if err != nil {
		return nil, err
	}

	req := run.Request()
	caller := req.Caller

	var callerSys *apisystem.System
	var callerDom *apidomain.Domain

	// Resolve the caller's System if it's been specified or default it to the
	// host system.
	if caller.System == "" || caller.System == d.hostSystemRecord.UUID {
		callerSys = d.hostSystemRecord
	} else {
		callerSys, err = d.systemStore.ReadByUUID(ctx, caller.System)
		if err != nil {
			return nil, err
		}
	}

	// Resolve the caller's Domain if it's been specified.
	if caller.Domain != "" {
		callerDom, err = d.domainStore.ReadByUUID(ctx, callerSys, caller.Domain)
		if err != nil {
			return nil, err
		}
	}

	target := req.Target
	targetKV = target.KindVersionName
	targetSys := target.System
	targetDom := target.Domain

	if targetDom != nil {
		targetSys = targetDom.System
	}

	// Default the target and caller system to the host system if it hasn't
	// been specified.
	if targetSys == nil {
		targetSys = d.hostSystemRecord
		if targetDom != nil {
			targetDom.System = targetSys
		}
	} else if !targetSys.HasSystemInternalID() {
		targetSys, err = d.systemStore.ReadByUUID(ctx, targetSys.UUID)
		if err != nil {
			return nil, err
		}
	}

	// Resolve the target domain if it's been specified.
	if targetDom != nil && !targetDom.HasSystemInternalID() {
		targetDom, err = d.domainStore.ReadByUUID(ctx, targetSys, targetDom.UUID)
		if err != nil {
			return nil, err
		}
	}

	targetKindRec, err := d.kindStore.ReadByName(
		ctx, targetSys, targetKV.Kind(),
	)
	if err != nil {
		if err == apierrors.ErrNotFound {
			return nil, apierrors.ErrKindUnknown
		}
		return nil, err
	}

	targetKVRec, err := d.kindversionStore.ReadByName(
		ctx, targetSys, targetKindRec, targetKV,
	)
	if err != nil {
		if err == apierrors.ErrNotFound {
			return nil, apierrors.ErrKindUnknown
		}
		return nil, err
	}
	targetRec, err := d.objectStore.ReadByUUIDAndGeneration(
		ctx, targetKVRec, target.UUID, target.Generation,
	)
	if err != nil {
		return nil, err
	}
	targetRec.Object.System = targetSys
	if targetDom != nil {
		targetRec.Object.Domain = targetDom
	}
	targetRec.Object.KindVersionName = targetKV

	rootIDs := run.Root

	if rootIDs != nil {
		rootUUID := rootIDs.UUID
		if rootUUID != "" && rootUUID != req.UUID {
			_, err = d.runStore.ReadByUUID(ctx, rootUUID)
			if err != nil {
				return nil, fmt.Errorf(
					"failed reading root run by uuid: %w", err,
				)
			}
		}
	}

	parentIDs := run.Parent
	if parentIDs != nil {
		rootUUID := rootIDs.UUID
		parentUUID := parentIDs.UUID
		// short-circuit if we've already looked up the root...
		if parentUUID == rootUUID {
			parentIDs = rootIDs
		} else {
			_, err = d.runStore.ReadByUUID(ctx, parentUUID)
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
		callerSys, callerDom,
		rootIDs, parentIDs, run,
	)
}

// runWriteValidate returns an error if the supplied run and write
// options are not valid for writing a single Run.
func (d *Driver) runWriteValidate(
	ctx context.Context,
	run apirun.Run,
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
	expr apiquery.Expression,
	opts ...apiquery.Option,
) (*apiquery.Result[*apirun.Run], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeRun),
		}
		if err != nil {
			attrs = append(attrs, apimetrics.AttributeErrCode(err))
		}
		apimetrics.InstrumentQueryRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		apimetrics.InstrumentQueryDuration.Record(ctx, elapsed)
	}()

	qopts := apiquery.NewOptions(opts...)
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
	resNewOpts := []apiquery.ResultModifier[*apirun.Run]{
		apiquery.ResultWithItems(recs),
		apiquery.ResultWithOptions[*apirun.Run](boundedOpts),
	}
	if len(recs) == int(boundedOpts.Limit()) {
		resNewOpts = append(
			resNewOpts,
			apiquery.ResultWithMarker[*apirun.Run](
				recs[len(recs)-1].UUID(),
			),
		)
	}
	return apiquery.NewResult[*apirun.Run](resNewOpts...), nil
}

// runQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) runQueryValidate(
	ctx context.Context,
	expr apiquery.Expression,
	opts apiquery.Options,
) error {
	return nil
}

// runQueryBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records queryed is
// less than the max page result.
func (d *Driver) runQueryBoundedOptions(
	ctx context.Context,
	opts apiquery.Options,
) apiquery.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultRunQueryLimit
	}
	limit = min(limit, MaxRunQueryLimit)
	return apiquery.NewOptions(apiquery.Limit(limit))
}
