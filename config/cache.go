package config

import (
	"fmt"

	"github.com/spf13/pflag"

	rxpconfig "github.com/relexec/rxp/config"
)

const (
	DefaultCacheSystemEnabled  = true
	flagCacheSystemEnabled     = "rxp-cache-system-enabled"
	flagCacheSystemEnabledDesc = "Enable the System cache."
	DefaultCacheSystemMaxSize  = "1Mb"
	flagCacheSystemMaxSize     = "rxp-cache-system-max-size"
	flagCacheSystemMaxSizeDesc = "Maximum size (in bytes) for the System cache. Specify with common size strings, e.g. '1MB' or '10Gb'"

	DefaultCacheDomainEnabled  = true
	flagCacheDomainEnabled     = "rxp-cache-domain-enabled"
	flagCacheDomainEnabledDesc = "Enable the Domain cache."
	DefaultCacheDomainMaxSize  = "2Mb"
	flagCacheDomainMaxSize     = "rxp-cache-domain-max-size"
	flagCacheDomainMaxSizeDesc = "Maximum size (in bytes) for the Domain cache. Specify with common size strings, e.g. '1MB' or '10Gb'"

	DefaultCacheNamespaceEnabled  = true
	flagCacheNamespaceEnabled     = "rxp-cache-namespace-enabled"
	flagCacheNamespaceEnabledDesc = "Enable the Namespace cache."
	DefaultCacheNamespaceMaxSize  = "8Mb"
	flagCacheNamespaceMaxSize     = "rxp-cache-namespace-max-size"
	flagCacheNamespaceMaxSizeDesc = "Maximum size (in bytes) for the Namespace cache. Specify with common size strings, e.g. '1MB' or '10Gb'"

	DefaultCacheMetaEnabled  = true
	flagCacheMetaEnabled     = "rxp-cache-meta-enabled"
	flagCacheMetaEnabledDesc = "Enable the Meta cache."
	DefaultCacheMetaMaxSize  = "32Mb"
	flagCacheMetaMaxSize     = "rxp-cache-meta-max-size"
	flagCacheMetaMaxSizeDesc = "Maximum size (in bytes) for the Meta cache. Specify with common size strings, e.g. '1MB' or '10Gb'"
)

// CacheConfigs contains configuration options for caches in the rxp-pg library.
type CacheConfigs struct {
	// System contains the configuration options for the rxp-pg library's
	// system cache.
	System CacheConfig `json:"system"`
	// Domain contains the configuration options for the rxp-pg library's
	// domain cache.
	Domain CacheConfig `json:"domain"`
	// Namespace contains the configuration options for the rxp-pg library's
	// namespace cache.
	Namespace CacheConfig `json:"namespace"`
	// Meta contains the configuration options for the rxp-pg library's
	// meta cache.
	Meta CacheConfig `json:"meta"`
}

// Validate checks for invalid settings.
func (c CacheConfigs) Validate() error {
	if c.Domain.Enabled {
		_, err := c.Domain.MaxSizeBytes()
		if err != nil {
			return fmt.Errorf("invalid domain cache max size: %w", err)
		}
	}
	if c.Meta.Enabled {
		_, err := c.Meta.MaxSizeBytes()
		if err != nil {
			return fmt.Errorf("invalid meta cache max size: %w", err)
		}
	}
	return nil
}

// CacheConfig contains configuration options for a single cache in the rxp-pg
// library.
type CacheConfig struct {
	// Enabled turns on this cache
	Enabled bool `json:"enabled"`
	// MaxSize is the maximum size (in bytes) for the cache. Specify with
	// common size strings, e.g. "1MB" or "10Gb".
	MaxSize string `json:"max_size"`
}

// MaxSizeBytes returns the MaxSize string value as the number of bytes.
func (c CacheConfig) MaxSizeBytes() (int, error) {
	size, err := rxpconfig.SizeFromString(c.MaxSize)
	if err != nil {
		return -1, err
	}
	return int(size.Bytes()), nil
}

// BindFlags binds the supplied flagset to the CacheConfigs' fields.
func (c *CacheConfigs) BindFlags(fs *pflag.FlagSet) {
	pflag.BoolVar(
		&c.System.Enabled,
		flagCacheSystemEnabled,
		DefaultCacheSystemEnabled,
		flagCacheSystemEnabledDesc,
	)
	pflag.StringVar(
		&c.System.MaxSize,
		flagCacheSystemMaxSize,
		DefaultCacheSystemMaxSize,
		flagCacheSystemMaxSizeDesc,
	)
	pflag.BoolVar(
		&c.Domain.Enabled,
		flagCacheDomainEnabled,
		DefaultCacheDomainEnabled,
		flagCacheDomainEnabledDesc,
	)
	pflag.StringVar(
		&c.Domain.MaxSize,
		flagCacheDomainMaxSize,
		DefaultCacheDomainMaxSize,
		flagCacheDomainMaxSizeDesc,
	)
	pflag.BoolVar(
		&c.Namespace.Enabled,
		flagCacheNamespaceEnabled,
		DefaultCacheNamespaceEnabled,
		flagCacheNamespaceEnabledDesc,
	)
	pflag.StringVar(
		&c.Namespace.MaxSize,
		flagCacheNamespaceMaxSize,
		DefaultCacheNamespaceMaxSize,
		flagCacheNamespaceMaxSizeDesc,
	)
	pflag.BoolVar(
		&c.Meta.Enabled,
		flagCacheMetaEnabled,
		DefaultCacheMetaEnabled,
		flagCacheMetaEnabledDesc,
	)
	pflag.StringVar(
		&c.Meta.MaxSize,
		flagCacheMetaMaxSize,
		DefaultCacheMetaMaxSize,
		flagCacheMetaMaxSizeDesc,
	)
}
