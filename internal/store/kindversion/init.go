package store

import (
	"context"
	"log/slog"

	"github.com/relexec/rxp-pg/internal/cache"
)

func (s *Store) init(ctx context.Context) error {
	s.Logger = s.Logger.With(slog.String("store", "kind"))

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
	if cfg.Cache.KindVersion.Enabled {
		s.Logger.Debug("initializing kindversion cache")
		cacheCfg := cfg.Cache.KindVersion
		byName, err := cache.New[byNameCacheKey, *Record](
			ctx,
			cache.WithConfig[byNameCacheKey, *Record](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.byName = byName
		s.OnClose(s.byName.Close)

		byRowID, err := cache.New[byRowIDCacheKey, byNameCacheKey](
			ctx,
			cache.WithConfig[byRowIDCacheKey, byNameCacheKey](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.byRowID = byRowID
		s.OnClose(s.byRowID.Close)
		s.Logger.Info("initialized kindversion cache")
	} else {
		s.Logger.Info("kindversion cache disabled")
	}
	return nil
}
