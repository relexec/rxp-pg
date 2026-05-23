package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/namespace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// NamespaceRead reads a Namespace from persistent storage.
func (d *Driver) NamespaceRead(
	ctx context.Context,
	sel namespace.Selector,
) (*namespace.Namespace, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeNamespace),
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

	err = d.namespaceReadValidate(ctx, sel)
	if err != nil {
		return nil, err
	}

	uuid := sel.UUID()
	if uuid != "" {
		rec, err := d.namespaceStore.ReadByUUID(ctx, uuid)
		if err != nil {
			return nil, err
		}
		return rec.Namespace, nil
	}

	name := sel.Name()
	dom := sel.Domain()

	rec, err := d.namespaceStore.ReadByName(
		ctx, dom, name,
	)
	if err != nil {
		return nil, err
	}
	return rec.Namespace, nil
}

// namespaceReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Namespace.
func (d *Driver) namespaceReadValidate(
	ctx context.Context,
	sel namespace.Selector,
) error {
	return sel.Validate()
}

// NamespaceWrite atomically writes the supplied Namespace to persistent storage.
func (d *Driver) NamespaceWrite(
	ctx context.Context,
	ns *namespace.Namespace,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeType(api.TypeNamespace),
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

	err = d.namespaceWriteValidate(ctx, ns)
	if err != nil {
		return err
	}
	return d.namespaceStore.Write(ctx, ns)
}

// namespaceWriteValidate returns an error if the supplied namespace and write
// options are not valid for writing a single Namespace.
func (d *Driver) namespaceWriteValidate(
	ctx context.Context,
	ns *namespace.Namespace,
) error {
	if ns == nil {
		return errors.RequiredParameterNil(
			"namespace",
			errors.WithWrap(errors.ErrInvalidWriteRequest),
		)
	}
	return ns.Validate()
}
