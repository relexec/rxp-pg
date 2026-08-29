package store

import (
	"context"
	"log/slog"

	rxpsystem "github.com/relexec/rxp/api/system"

	"github.com/relexec/rxp-pg/internal/cache"
)

func (s *Store) init(ctx context.Context) error {
	s.Logger = s.Logger.With(slog.String("store", "system"))

	err := s.Config.Validate()
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
	cfg := s.Config
	if cfg.Cache.System.Enabled {
		s.Logger.Debug("initializing system cache")

		s.cacheLock.Lock()
		defer s.cacheLock.Unlock()

		cacheCfg := cfg.Cache.System
		byUUID, err := cache.New[byUUIDCacheKey, *rxpsystem.System](
			ctx,
			cache.WithConfig[byUUIDCacheKey, *rxpsystem.System](cacheCfg),
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
		s.Logger.Info("initialized system cache")
	} else {
		s.Logger.Info("system cache disabled")
	}
	return nil
}
