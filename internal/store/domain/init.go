package store

import (
	"context"

	"github.com/relexec/rxp-pg/internal/cache"
)

func (s *Store) init(ctx context.Context) error {
	s.SetLogger(s.Logger().WithName("rxp.pg.store.domain"))

	err := s.Config().Validate()
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
	cfg := s.Config()
	if cfg.Cache.Domain.Enabled {
		s.Debug("initializing domain cache")

		s.cacheLock.Lock()
		defer s.cacheLock.Unlock()

		cacheCfg := cfg.Cache.Domain
		byUUID, err := cache.New[byUUIDCacheKey, *Record](
			ctx,
			cache.WithConfig[byUUIDCacheKey, *Record](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.byUUID = byUUID
		s.OnClose(s.byUUID.Close)

		byName, err := cache.New[byNameCacheKey, byUUIDCacheKey](
			ctx,
			cache.WithConfig[byNameCacheKey, byUUIDCacheKey](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.byName = byName
		s.OnClose(s.byName.Close)

		byRowID, err := cache.New[byRowIDCacheKey, byUUIDCacheKey](
			ctx,
			cache.WithConfig[byRowIDCacheKey, byUUIDCacheKey](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.byRowID = byRowID
		s.OnClose(s.byRowID.Close)
		s.Info("initialized domain cache")
	} else {
		s.Debug("domain cache disabled")
	}
	return nil
}
