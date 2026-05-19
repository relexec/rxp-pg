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
	s.log.WithName("rxp.pg.store.meta")

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
	if s.cfg.Cache.Meta.Enabled {
		s.log.V(4).Info("initializing meta cache")
		cacheCfg := s.cfg.Cache.Meta
		byKindVersion, err := cache.New[byKindVersionCacheKey, *Record](
			ctx,
			cache.WithConfig[byKindVersionCacheKey, *Record](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.byKindVersion = byKindVersion
		s.onClose = append(s.onClose, s.byKindVersion.Close)

		byRowID, err := cache.New[byRowIDCacheKey, byKindVersionCacheKey](
			ctx,
			cache.WithConfig[byRowIDCacheKey, byKindVersionCacheKey](cacheCfg),
		)
		if err != nil {
			return err
		}
		s.byRowID = byRowID
		s.onClose = append(s.onClose, s.byRowID.Close)
		s.log.Info("initialized meta cache")
	} else {
		s.log.V(4).Info("meta cache disabled")
	}
	return nil
}
