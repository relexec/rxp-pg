package store

import (
	"context"

	"github.com/go-logr/logr"

	"github.com/relexec/rxp-pg/config"
)

func (s *Store) init(ctx context.Context) error {
	if s.cfg == nil {
		s.cfg = config.Default()
	}
	if s.log == nil {
		lc := logr.FromContextOrDiscard(ctx)
		s.log = &lc
	}
	s.log.WithName("rxp.pg.store.namespace")

	err := s.cfg.Validate()
	if err != nil {
		return err
	}
	return nil
}
