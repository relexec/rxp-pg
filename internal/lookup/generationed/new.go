package generationed

import (
	"context"

	"github.com/relexec/rxp-pg/config"
)

// WithOption allows configuring the returned Lookup.
type WithOption func(*Lookup)

// New returns a new Lookup.
func New(
	ctx context.Context,
	opts ...WithOption,
) (*Lookup, error) {
	l := &Lookup{}
	for _, opt := range opts {
		opt(l)
	}
	if err := l.init(ctx); err != nil {
		return nil, err
	}
	return l, nil
}

// WithType sets the Lookup's Type.
func WithType(typ string) WithOption {
	return func(l *Lookup) {
		l.typ = typ
	}
}

// WithConfig sets the Lookup's configuration.
func WithConfig(cfg config.CacheConfig) WithOption {
	return func(l *Lookup) {
		l.cfg = cfg
	}
}
