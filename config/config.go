package config

import (
	"fmt"
	"runtime"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/pflag"
)

const (
	flagSystemUUID     = "rxp-system-uuid"
	flagSystemUUIDDesc = "Contains the rxp host system UUID. If empty, the value of RXP_SYSTEM_UUID environs variable is used."
	flagSystemName     = "rxp-system-name"
	flagSystemNameDesc = "Contains the rxp host system name. If empty, the value of RXP_SYSTEM_NAME environs variable is used."

	flagConnect     = "rxp-postgres-connect"
	flagConnectDesc = "Contains the libpq connection string in either key=value or URL format. If not empty, all connection parameters are set from the supplied string."
)

var (
	DefaultMaxConnections = max(4, runtime.NumCPU())
)

const (
	flagMaxConnections     = "rxp-postgres-max-connections"
	flagMaxConnectionsDesc = "The max size of the connection pool. Defaults to the greater of 4 or runtime.NumCPU()."
)

// Config contains configuration options for the rxp-pg library.
type Config struct {
	// SystemUUID contains the rxp host system UUID.
	SystemUUID string `json:"system_uuid,omitempty"`
	// SystemName contains the rxp host system Name, if any.
	SystemName string `json:"system_name,omitempty"`
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
	pflag.StringVar(
		&c.SystemUUID,
		flagSystemUUID,
		"",
		flagSystemUUIDDesc,
	)
	pflag.StringVar(
		&c.SystemName,
		flagSystemName,
		"",
		flagSystemNameDesc,
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
	c.Cache.BindFlags(fs)
}
