package store

import (
	"context"
	"log/slog"
)

func (s *Store) init(ctx context.Context) error {
	s.Logger = s.Logger.With(slog.String("store", "object"))

	err := s.Config.Validate()
	if err != nil {
		return err
	}
	return nil
}
