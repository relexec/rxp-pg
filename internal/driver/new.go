package driver

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/relexec/rxp/api/metrics"

	"github.com/relexec/rxp-pg/config"
)

type WithOption func(*Driver)

// New returns a new Driver.
func New(
	ctx context.Context,
	cfg config.Config,
	opts ...WithOption,
) (*Driver, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("failed initializing driver: %w", err)
	}
	d := &Driver{
		Config: cfg,
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.Logger == nil {
		d.Logger = cfg.Log.Logger()
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

// WithLogger sets the Driver's Logger to the supplied value.
func WithLogger(logger *slog.Logger) WithOption {
	return func(d *Driver) {
		d.Logger = logger
	}
}

// WithMetrics sets the Driver's Metrics handler to the supplied value.
func WithMetrics(h *metrics.Handler) WithOption {
	return func(d *Driver) {
		d.Metrics = h
	}
}
