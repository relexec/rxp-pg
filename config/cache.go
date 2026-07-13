package config

import (
	"fmt"

	"github.com/spf13/pflag"

	"github.com/relexec/pkg/size"
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
	DefaultCacheDomainMaxSize  = "4Mb"
	flagCacheDomainMaxSize     = "rxp-cache-domain-max-size"
	flagCacheDomainMaxSizeDesc = "Maximum size (in bytes) for the Domain cache. Specify with common size strings, e.g. '1MB' or '10Gb'"

	DefaultCacheKindEnabled  = true
	flagCacheKindEnabled     = "rxp-cache-kind-enabled"
	flagCacheKindEnabledDesc = "Enable the Kind cache."
	DefaultCacheKindMaxSize  = "2Mb"
	flagCacheKindMaxSize     = "rxp-cache-kind-max-size"
	flagCacheKindMaxSizeDesc = "Maximum size (in bytes) for the Kind cache. Specify with common size strings, e.g. '1MB' or '10Gb'"

	DefaultCacheKindVersionEnabled  = true
	flagCacheKindVersionEnabled     = "rxp-cache-kindversion-enabled"
	flagCacheKindVersionEnabledDesc = "Enable the KindVersion cache."
	DefaultCacheKindVersionMaxSize  = "4Mb"
	flagCacheKindVersionMaxSize     = "rxp-cache-kindversion-max-size"
	flagCacheKindVersionMaxSizeDesc = "Maximum size (in bytes) for the KindVersion cache. Specify with common size strings, e.g. '1MB' or '10Gb'"
)

// CacheConfigs contains configuration options for caches in the rxp-pg library.
type CacheConfigs struct {
	// System contains the configuration options for the rxp-pg library's
	// system cache.
	System CacheConfig `json:"system"`
	// Domain contains the configuration options for the rxp-pg library's
	// domain cache.
	Domain CacheConfig `json:"domain"`
	// Kind contains the configuration options for the rxp-pg library's
	// kind cache.
	Kind CacheConfig `json:"kind"`
	// KindVersion contains the configuration options for the rxp-pg library's
	// kindversion cache.
	KindVersion CacheConfig `json:"kindversion"`
}

// Validate checks for invalid settings.
func (c CacheConfigs) Validate() error {
	if c.System.Enabled {
		_, err := c.System.MaxSizeBytes()
		if err != nil {
			return fmt.Errorf("invalid system cache max size: %w", err)
		}
	}
	if c.Domain.Enabled {
		_, err := c.Domain.MaxSizeBytes()
		if err != nil {
			return fmt.Errorf("invalid domain cache max size: %w", err)
		}
	}
	if c.Kind.Enabled {
		_, err := c.Kind.MaxSizeBytes()
		if err != nil {
			return fmt.Errorf("invalid kind cache max size: %w", err)
		}
	}
	if c.KindVersion.Enabled {
		_, err := c.KindVersion.MaxSizeBytes()
		if err != nil {
			return fmt.Errorf("invalid kindversion cache max size: %w", err)
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
	s, err := size.FromString(c.MaxSize)
	if err != nil {
		return -1, err
	}
	return int(s.Bytes()), nil
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
		&c.Kind.Enabled,
		flagCacheKindEnabled,
		DefaultCacheKindEnabled,
		flagCacheKindEnabledDesc,
	)
	pflag.StringVar(
		&c.Kind.MaxSize,
		flagCacheKindMaxSize,
		DefaultCacheKindMaxSize,
		flagCacheKindMaxSizeDesc,
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
		&c.KindVersion.Enabled,
		flagCacheKindVersionEnabled,
		DefaultCacheKindVersionEnabled,
		flagCacheKindVersionEnabledDesc,
	)
	pflag.StringVar(
		&c.KindVersion.MaxSize,
		flagCacheKindVersionMaxSize,
		DefaultCacheKindVersionMaxSize,
		flagCacheKindVersionMaxSizeDesc,
	)
}
