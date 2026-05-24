package driver

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/relexec/rxp/metrics"

	"github.com/relexec/rxp-pg/config"
)

type WithOption func(*Driver)

// New returns a new Driver.
func New(
	ctx context.Context,
	opts ...WithOption,
) (*Driver, error) {
	d := &Driver{}
	for _, opt := range opts {
		opt(d)
	}
	if err := d.init(ctx); err != nil {
		return nil, err
	}
	return d, nil
}

// WithHostSystemUUID sets the Driver's host system UUID.
func WithHostSystemUUID(uuid string) WithOption {
	return func(d *Driver) {
		d.hostSystemUUID = uuid
	}
}

// WithHostSystemTag sets the Driver's host system tag.
func WithHostSystemTag(tag string) WithOption {
	return func(d *Driver) {
		d.hostSystemTag = tag
	}
}

// WithConfig sets the Driver's Config to the supplied value.
func WithConfig(cfg *config.Config) WithOption {
	return func(d *Driver) {
		d.cfg = cfg
	}
}

// WithLogger sets the Driver's Logger to the supplied value.
func WithLogger(logger logr.Logger) WithOption {
	return func(d *Driver) {
		d.log = &logger
	}
}

// WithMetrics sets the Driver's Metrics handler to the supplied value.
func WithMetrics(metrics *metrics.Metrics) WithOption {
	return func(d *Driver) {
		d.metrics = metrics
	}
}
