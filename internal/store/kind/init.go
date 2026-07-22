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
	if cfg.Cache.Kind.Enabled {
		s.Logger.Debug("initializing kind cache")
		cacheCfg := cfg.Cache.Kind
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
		s.Logger.Info("initialized kind cache")
	} else {
		s.Logger.Info("kind cache disabled")
	}
	return nil
}
