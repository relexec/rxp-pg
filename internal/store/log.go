package store

import "github.com/relexec/rxp/log"

// debug writes a debug-level message to the Store's configured logger.
func (s *Store) debug(msg string, keysAndValues ...any) {
	if s.log == nil {
		return
	}
	s.log.V(log.LevelDebug).Info(msg, keysAndValues...)
}

// info writes a info-level message to the Store's configured logger.
func (s *Store) info(msg string, keysAndValues ...any) {
	if s.log == nil {
		return
	}
	s.log.V(log.LevelInfo).Info(msg, keysAndValues...)
}
