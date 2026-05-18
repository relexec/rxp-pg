package driver

import (
	"context"
	"time"

	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/namespace/write"
	writeoption "github.com/relexec/rxp/namespace/write/option"
	readoption "github.com/relexec/rxp/read/option"
	"github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// NamespaceRead reads a Namespace from persistent storage.
func (d *Driver) NamespaceRead(
	ctx context.Context,
	sel types.Selector,
	opts ...readoption.Option,
) (types.Namespace, error) {
	err := d.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeTargetType(metrics.TargetTypeNamespace),
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

	ropts := readoption.New(opts...)
	err = d.namespaceReadValidate(ctx, sel, ropts)
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
	dom := name.Domain()

	rec, err := d.namespaceStore.ReadByName(
		ctx, dom, types.NamespaceName(name.Name()),
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
	sel types.Selector,
	opts readoption.Options,
) error {
	err := sel.Validate()
	if err != nil {
		return err
	}
	// If we're not looking up by UUID, verify that the name supplied in the
	// selector is a valid NamespaceName and has a non-nil Domain.
	if sel.UUID() != "" {
		return nil
	}
	dom := sel.Name().Domain()
	if dom == nil {
		return errors.ErrSelectorDomainRequired
	}
	err = dom.Validate()
	if err != nil {
		return err
	}
	n := sel.Name().Name()
	nn := types.NamespaceName(n)
	return nn.Validate()
}

// NamespaceWrite atomically writes the supplied Namespace to persistent storage.
func (d *Driver) NamespaceWrite(
	ctx context.Context,
	ns types.Namespace,
	opts ...writeoption.Option,
) error {
	err := d.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeTargetType(metrics.TargetTypeNamespace),
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

	wopts := writeoption.New(opts...)
	err = d.namespaceWriteValidate(ctx, ns, wopts)
	if err != nil {
		return err
	}
	return d.namespaceStore.Write(ctx, ns)
}

// namespaceWriteValidate returns an error if the supplied namespace and write
// options are not valid for writing a single Namespace.
func (d *Driver) namespaceWriteValidate(
	ctx context.Context,
	namespace types.Namespace,
	opts writeoption.Options,
) error {
	return namespace.Validate()
}

// var _ read.NamespaceReader = (*Driver)(nil)
var _ write.NamespaceWriter = (*Driver)(nil)
