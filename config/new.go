package config

import "github.com/spf13/pflag"

// WithOption modifies a Config returned from New.
type WithOption func(*Config)

// New returns a new Config.
func New(opts ...WithOption) *Config {
	c := &Config{}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// WithFlags binds the supplied flags to the Config's fields.
func WithFlags(fs *pflag.FlagSet) WithOption {
	return func(c *Config) {
		c.BindFlags(fs)
	}
}

// WithConnect sets the Config's Connect field.
func WithConnect(connect string) WithOption {
	return func(c *Config) {
		c.Connect = connect
	}
}

// WithMaxConnections sets the Config's MaxConnections field.
func WithMaxConnections(count int) WithOption {
	return func(c *Config) {
		c.MaxConnections = count
	}
}

// Default returns the default Config to be used if no options are specified.
func Default() *Config {
	return &Config{
		MaxConnections: DefaultMaxConnections,
	}
}
