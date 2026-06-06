package store

import (
	"context"
)

func (s *Store) init(ctx context.Context) error {
	s.SetLogger(s.Logger().WithName("rxp.pg.store.object"))

	err := s.Config().Validate()
	if err != nil {
		return err
	}
	return nil
}
