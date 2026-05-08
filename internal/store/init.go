package store

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/system"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/cache"
)

func (s *Store) init(ctx context.Context) error {
	if s.cfg == nil {
		s.cfg = config.Default()
	}
	if s.log == nil {
		lc := logr.FromContextOrDiscard(ctx)
		s.log = &lc
	}
	s.log.WithName("rxp.pg.store")

	err := s.cfg.Validate()
	if err != nil {
		return err
	}
	if s.hostSystemUUID == "" {
		// try to find the system identifier by looking at the configuration
		// and environment variables.
		if s.cfg.SystemID != "" {
			s.hostSystemUUID = s.cfg.SystemID
		} else {
			v := os.Getenv("RXP_SYSTEM_ID")
			if v == "" {
				return fmt.Errorf(
					"Unable to determine rxp host system identifier",
				)
			}
			s.hostSystemUUID = v
		}
	}
	s.log.Info("host system identifier: %s", s.hostSystemUUID)

	if s.cfg.Cache.System.Enabled {
		s.log.V(4).Info("initializing system cache")
		cacheCfg := s.cfg.Cache.System
		sc, err := cache.New[systemCacheKey, *systemEntry](
			ctx,
			cache.WithConfig[systemCacheKey, *systemEntry](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.systemCache = sc
		s.onClose = append(s.onClose, s.systemCache.Close)
		s.log.Info("initialized system cache")
	} else {
		s.log.V(4).Info("system cache disabled")
	}

	s.log.V(4).Info("initializing pgxpool connections")
	poolConfig, err := s.cfg.PGXPoolConfig()
	if err != nil {
		return err
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return fmt.Errorf("failed connecting to postgres: %w", err)
	}
	s.log.Info("initialized pgxpool connections")
	s.pool = pool

	s.log.V(4).Info("initializing host system record")
	if s.hostSystem == nil {
		entry, err := s.systemDBRead(ctx, s.hostSystemUUID)
		if err != nil {
			if err != errors.ErrNotFound {
				return err
			}
			s.log.V(4).Info("creating host system record")
			err = s.systemDBWrite(
				ctx,
				system.New(system.WithUUID(s.hostSystemUUID)),
			)
			if err != nil {
				return err
			}
			s.log.Info("created host system record")
			// Populate the read-through cache for our host system
			entry, err = s.systemDBRead(ctx, s.hostSystemUUID)
			if err != nil {
				return err
			}
		}
		s.hostSystem = entry
	}
	s.log.Info("host system record initialized")

	s.log.V(4).Info("initializing metrics")
	if s.metrics == nil {
		metrics, err := metrics.New(ctx)
		if err != nil {
			return err
		}
		s.metrics = metrics
	}
	s.onClose = append(s.onClose, s.metrics.MeterProvider().Shutdown)
	err = metrics.Init(s.metrics)
	if err != nil {
		return fmt.Errorf("failed initializing metrics: %w", err)
	}
	s.log.Info("initialized metrics")

	if s.cfg.Cache.Domain.Enabled {
		s.log.V(4).Info("initializing domain cache")
		cacheCfg := s.cfg.Cache.Domain
		dc, err := cache.New[domainCacheKey, *domainEntry](
			ctx,
			cache.WithConfig[domainCacheKey, *domainEntry](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.domainCache = dc
		s.onClose = append(s.onClose, s.domainCache.Close)
		s.log.Info("initialized domain cache")
	} else {
		s.log.V(4).Info("domain cache disabled")
	}

	if s.cfg.Cache.Namespace.Enabled {
		s.log.V(4).Info("initializing namespace cache")
		cacheCfg := s.cfg.Cache.Namespace
		nc, err := cache.New[namespaceCacheKey, *namespaceEntry](
			ctx,
			cache.WithConfig[namespaceCacheKey, *namespaceEntry](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.namespaceCache = nc
		s.onClose = append(s.onClose, s.namespaceCache.Close)
		s.log.Info("initialized namespace cache")
	} else {
		s.log.V(4).Info("namespace cache disabled")
	}

	if s.cfg.Cache.Meta.Enabled {
		s.log.V(4).Info("initializing meta cache")
		cacheCfg := s.cfg.Cache.Meta
		mc, err := cache.New[metaCacheKey, *metaEntry](
			ctx,
			cache.WithConfig[metaCacheKey, *metaEntry](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.metaCache = mc
		s.onClose = append(s.onClose, s.metaCache.Close)
		s.log.Info("initialized meta cache")
	} else {
		s.log.V(4).Info("meta cache disabled")
	}
	return nil
}
