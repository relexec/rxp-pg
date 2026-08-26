package driver

import (
	"context"
	"time"

	apicore "github.com/relexec/rxp/api/core"
	apidomain "github.com/relexec/rxp/api/domain"
	apierrors "github.com/relexec/rxp/api/errors"
	apimetrics "github.com/relexec/rxp/api/metrics"
	apiquery "github.com/relexec/rxp/api/query"
	apisystem "github.com/relexec/rxp/api/system"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// DomainRead reads a Domain from persistent storage.
func (d *Driver) DomainRead(
	ctx context.Context,
	sel apidomain.Selector,
) (*apidomain.Domain, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeDomain),
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

	err = d.domainReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	var sysRec *apisystem.System

	sys := sel.System()

	// Default the system to the host system if it hasn't been specified in the
	// selector.
	if sys == nil {
		sys = d.hostSystemRecord
	}

	if sys.UUID != d.hostSystemUUID {
		sysRec, err = d.systemStore.ReadByUUID(ctx, sys.UUID)
		if err != nil {
			if err == apierrors.ErrNotFound {
				return nil, apierrors.ErrSystemUnknown
			}
			return nil, err
		}
	} else {
		sysRec = d.hostSystemRecord
	}

	uuid := sel.UUID()
	if uuid != "" {
		return d.domainStore.ReadByUUID(ctx, sysRec, uuid)
	}

	name := sel.Name()
	return d.domainStore.ReadByName(ctx, sysRec, name)
}

// domainReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Domain.
func (d *Driver) domainReadValidate(
	ctx context.Context,
	sel apidomain.Selector,
) error {
	return sel.Validate()
}

// domainRecordFromDomain examines the supplied Domain and returns the
// associated Record from the domain store.
func (d *Driver) domainRecordFromDomain(
	ctx context.Context,
	sysRec *apisystem.System,
	dom *apidomain.Domain,
) (*apidomain.Domain, error) {
	if dom == nil {
		return nil, nil
	}
	if dom.UUID != "" {
		return d.domainStore.ReadByUUID(
			ctx, sysRec, dom.UUID,
		)
	}
	return d.domainStore.ReadByName(
		ctx, sysRec, dom.Name,
	)
}

// DomainWrite atomically writes the supplied Domain to persistent storage.
func (d *Driver) DomainWrite(
	ctx context.Context,
	dom apidomain.Domain,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeDomain),
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

	err = d.domainWriteValidate(ctx, dom)
	if err != nil {
		return err
	}

	var sysRec *apisystem.System

	sys := dom.System

	// Default the system to the host system if it hasn't been specified in the
	// selector.
	if sys == nil {
		sys = d.hostSystemRecord
		dom.System = sys
	}

	if sys.UUID != d.hostSystemUUID {
		sysRec, err = d.systemStore.ReadByUUID(ctx, sys.UUID)
		if err != nil {
			if err == apierrors.ErrNotFound {
				return apierrors.ErrSystemUnknown
			}
			return err
		}
	} else {
		sysRec = d.hostSystemRecord
	}

	// if we're creating/updating a non-root domain, we need to ensure the
	// parent domain exists.
	parDom := dom.Parent
	if parDom != nil {
		parRowID := parDom.SystemInternalIDInt64()
		parUUID := parDom.UUID
		if parDom.HasSystemInternalID() {
			parDom, err = d.domainStore.ReadByRowID(ctx, sysRec, parRowID)
		} else if parUUID != "" {
			parDom, err = d.domainStore.ReadByUUID(ctx, sysRec, parUUID)
		} else {
			parName := parDom.Name
			parDom, err = d.domainStore.ReadByName(ctx, sysRec, parName)
		}
		if err != nil {
			if err == apierrors.ErrNotFound {
				return apierrors.ErrDomainParentNotFound
			}
			return err
		}
		dom.Parent = parDom
	}

	return d.domainStore.Write(ctx, sysRec, dom)
}

// domainWriteValidate returns an error if the supplied domain and write
// options are not valid for writing a single Domain.
func (d *Driver) domainWriteValidate(
	ctx context.Context,
	dom apidomain.Domain,
) error {
	return dom.Validate()
}

const (
	DefaultDomainQueryLimit = 10
	MaxDomainQueryLimit     = 100
)

// DomainQuery queries zero or more Domains from persistent storage.
func (d *Driver) DomainQuery(
	ctx context.Context,
	expr apiquery.Expression,
	opts ...apiquery.Option,
) (*apiquery.Result[*apidomain.Domain], error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			apimetrics.AttributeType(apicore.TypeDomain),
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
	err = d.domainQueryValidate(ctx, expr, qopts)
	if err != nil {
		return nil, err
	}

	boundedOpts := d.domainQueryBoundedOptions(ctx, qopts)

	recs, err := d.domainStore.Query(
		ctx, expr, boundedOpts,
	)
	if err != nil {
		return nil, err
	}
	resOpts := apiquery.NewOptions(
		apiquery.Limit(boundedOpts.Limit()),
	)
	if len(recs) == int(boundedOpts.Limit()) {
		resOpts = apiquery.NewOptions(
			apiquery.ContinueFrom(recs[len(recs)-1].UUID),
			apiquery.Limit(boundedOpts.Limit()),
		)
	}
	resNewOpts := []apiquery.ResultModifier[*apidomain.Domain]{
		apiquery.ResultWithItems(recs),
		apiquery.ResultWithOptions[*apidomain.Domain](resOpts),
	}
	return apiquery.NewResult[*apidomain.Domain](resNewOpts...), nil
}

// domainQueryValidate returns an error if the supplied expression and query
// options are not valid.
func (d *Driver) domainQueryValidate(
	ctx context.Context,
	expr apiquery.Expression,
	opts apiquery.Options,
) error {
	if expr == nil {
		return apierrors.ErrQueryExpressionRequired
	}
	return nil
}

// domainQueryBoundedOptions returns a Options that has been bounded with
// reasonable defaults, e.g. ensuring that the number of records queryed is
// less than the max page result.
func (d *Driver) domainQueryBoundedOptions(
	ctx context.Context,
	opts apiquery.Options,
) apiquery.Options {
	limit := opts.Limit()
	if limit <= 0 {
		limit = DefaultDomainQueryLimit
	}
	limit = min(limit, MaxDomainQueryLimit)
	return apiquery.NewOptions(apiquery.Limit(limit))
}
