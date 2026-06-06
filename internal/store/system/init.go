package store

import (
	"context"

	"github.com/go-logr/logr"

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
	s.log.WithName("rxp.pg.store.system")

	err := s.cfg.Validate()
	if err != nil {
		return err
	}

	if err = s.initCache(ctx); err != nil {
		return err
	}
	return nil
}

// initCache initializes the lookup caches if they are enabled in our
// configuration.
func (s *Store) initCache(ctx context.Context) error {
	if s.cfg.Cache.System.Enabled {
		s.log.V(4).Info("initializing system cache")
		cacheCfg := s.cfg.Cache.System
		byUUID, err := cache.New[byUUIDCacheKey, *Record](
			ctx,
			cache.WithConfig[byUUIDCacheKey, *Record](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.byUUID = byUUID
		s.OnClose(s.byUUID.Close)

		byRowID, err := cache.New[byRowIDCacheKey, byUUIDCacheKey](
			ctx,
			cache.WithConfig[byRowIDCacheKey, byUUIDCacheKey](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.byRowID = byRowID
		s.OnClose(s.byRowID.Close)
		s.log.Info("initialized system cache")
	} else {
		s.log.V(4).Info("system cache disabled")
	}
	return nil
}
