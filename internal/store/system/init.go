package store

import (
	"context"
	"log/slog"

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
		cacheCfg := cfg.Cache.System
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
		s.Logger.Info("initialized system cache")
	} else {
		s.Logger.Info("system cache disabled")
	}
	return nil
}
