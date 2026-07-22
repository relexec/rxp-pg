package config

import (
	"fmt"
	"os"
	"runtime"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	pkglog "github.com/relexec/pkg/log"
	"github.com/spf13/pflag"
)

const (
	flagSystemUUID     = "rxp-system-uuid"
	flagSystemUUIDDesc = "Contains the rxp host system UUID. If empty, the value of RXP_SYSTEM_UUID environs variable is used."
	envVarSystemUUID   = "RXP_SYSTEM_UUID"
	flagSystemTag      = "rxp-system-tag"
	flagSystemTagDesc  = "Contains the rxp host system tag. If empty, the value of RXP_SYSTEM_TAG environs variable is used."
	envVarSystemTag    = "RXP_SYSTEM_TAG"
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
	// SystemUUID contains the rxp host system UUID.
	SystemUUID string `json:"system_uuid,omitempty"`
	// SystemTag contains the rxp host system Name, if any.
	SystemTag string `json:"system_name,omitempty"`
	// Log contains options for configuring logging.
	Log pkglog.Config `json:"log"`
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
	if c.SystemUUID == "" {
		c.SystemUUID = os.Getenv(envVarSystemUUID)
	}
	if c.SystemTag == "" {
		c.SystemTag = os.Getenv(envVarSystemTag)
	}
	if c.Connect == "" {
		c.Connect = os.Getenv(EnvVarConnect)
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = DefaultMaxConnections
	}
	c.Cache.SetDefaults()
	c.Log.SetDefaults()
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
	if err := c.Log.Validate(); err != nil {
		return err
	}
	return c.Cache.Validate()
}

// BindFlags bings the supplied flagset to the Config's fields.
func (c *Config) BindFlags(fs *pflag.FlagSet) {
	pflag.StringVar(
		&c.SystemUUID,
		flagSystemUUID,
		"",
		flagSystemUUIDDesc,
	)
	pflag.StringVar(
		&c.SystemTag,
		flagSystemTag,
		"",
		flagSystemTagDesc,
	)
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
	c.Log.BindFlags(fs)
	c.Cache.BindFlags(fs)
}
