package config

import (
	"fmt"
	"os"
	"runtime"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	rxpconfig "github.com/relexec/rxp/config"
	"github.com/spf13/pflag"
)

const (
	flagConnect     = "rxp-pg-connect"
	flagConnectDesc = "Contains the libpq connection string in either key=value or URL format. If not empty, all connection parameters are set from the supplied string."
	EnvVarConnect   = "RXP_PG_CONNECT"
)

var (
	DefaultMaxConnections = max(4, runtime.NumCPU())
)

const (
	flagMaxConnections     = "rxp-pg-max-connections"
	flagMaxConnectionsDesc = "The max size of the connection pool. Defaults to the greater of 4 or runtime.NumCPU()."
)

// Config contains configuration options for the rxp-pg library.
type Config struct {
	rxpconfig.Config
	// Connect contains the libpq connection string in either key=value or URL
	// format.
	//
	// If not empty, all connection parameters are set from the string instead
	// of individual Config fields.
	//
	// If empty, the connection string is constructed from individual Config
	// fields.
	//
	// Ref: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
	Connect string `json:"connect,omitempty"`
	// MaxConnections is the max size of the connection pool.
	//
	// Defaults to the greater of 4 or runtime.NumCPU().
	MaxConnections int `json:"max_connections,omitempty"`
	// Cache contains the configuration options for the rxp-pg's caches.
	Cache CacheConfigs `json:"cache,omitempty"`
}

// SetDefaults sets any missing values to their defaults or environs variable
// values.
func (c *Config) SetDefaults() {
	c.Config.SetDefaults()
	if c.Connect == "" {
		c.Connect = os.Getenv(EnvVarConnect)
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = DefaultMaxConnections
	}
}

// PGXPoolConfig returns the Config as a [pgxpool.Config]
func (c Config) PGXPoolConfig() (*pgxpool.Config, error) {
	if c.Connect != "" {
		pc, err := pgxpool.ParseConfig(c.Connect)
		if err != nil {
			return nil, fmt.Errorf("invalid postgres Connect: %w", err)
		}
		return pc, nil
	}
	return nil, nil
}

// Validate checks for invalid settings.
func (c Config) Validate() error {
	if c.Connect != "" {
		_, err := pgconn.ParseConfig(c.Connect)
		if err != nil {
			return fmt.Errorf("invalid postgres Connect: %w", err)
		}
	}
	return c.Cache.Validate()
}

// BindFlags bings the supplied flagset to the Config's fields.
func (c *Config) BindFlags(fs *pflag.FlagSet) {
	c.Config.BindFlags(fs)
	pflag.StringVar(
		&c.Connect,
		flagConnect,
		"",
		flagConnectDesc,
	)
	pflag.IntVar(
		&c.MaxConnections,
		flagMaxConnections,
		DefaultMaxConnections,
		flagMaxConnectionsDesc,
	)
	c.Cache.BindFlags(fs)
}
