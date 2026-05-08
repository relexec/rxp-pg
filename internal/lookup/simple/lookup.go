package simple

import (
	"context"
	"unsafe"

	"github.com/dgraph-io/ristretto/v2"

	"github.com/relexec/rxp-pg/config"
)

const (
	// Ref: https://github.com/dgraph-io/ristretto/blob/402101df6c698ed1253bb305ce9cda71bc83ad1d/cache.go#L114-L122
	defaultBufferItems = int64(64)
)

// Lookup manages reverse lookups of string Name <-> integer DB Row ID
// map entries.
type Lookup struct {
	// type is the Type of thing stored in the Lookup.
	typ string
	// cfg stores the Lookup's configuration.
	cfg config.CacheConfig
	// nameCache is the cache, keyed by row ID, valued by name.
	nameCache *ristretto.Cache[int64, string]
	// idCache is the cache, keyed by name, valued by row ID.
	idCache *ristretto.Cache[string, int64]
}

// Type returns the string name of the type of thing the Lookup stores.
func (l Lookup) Type() string {
	return l.typ
}

// Close ensures the Lookup drains properly.
func (l *Lookup) Close(ctx context.Context) error {
	if l.nameCache != nil {
		l.nameCache.Close()
	}
	if l.idCache != nil {
		l.idCache.Close()
	}
	return nil
}

// RowID returns the row ID given the supplied name.
func (l Lookup) RowID(name string) (int64, bool) {
	if l.idCache == nil {
		return -1, false
	}
	return l.idCache.Get(name)
}

// Name returns the name given the supplied row ID.
func (l Lookup) Name(id int64) (string, bool) {
	if l.nameCache == nil {
		return "", false
	}
	return l.nameCache.Get(id)
}

// Set sets the supplied name and row ID entry into the Lookup,
// returning whether the insertion occurred.
func (l *Lookup) Set(name string, id int64) bool {
	if l.nameCache == nil || l.idCache == nil {
		return false
	}

	// Cost for all entries is the same: 1.
	insName := l.nameCache.Set(id, name, 1)
	insID := l.idCache.Set(name, id, 1)
	l.nameCache.Wait()
	l.idCache.Wait()
	return insName && insID
}

// init sets up the Lookup, initializing the underlying ristretto cache and
// metrics.
func (l *Lookup) init(ctx context.Context) error {
	maxSize, err := l.cfg.MaxSizeBytes()
	if err != nil {
		return err
	}
	nc, err := ristretto.NewCache(
		&ristretto.Config[int64, string]{
			NumCounters: numCounters(maxSize, unsafe.Sizeof("")),
			MaxCost:     int64(maxSize),
			BufferItems: defaultBufferItems,
		},
	)
	if err != nil {
		return err
	}
	l.nameCache = nc
	ic, err := ristretto.NewCache(
		&ristretto.Config[string, int64]{
			NumCounters: numCounters(maxSize, unsafe.Sizeof(int64(0))),
			MaxCost:     int64(maxSize),
			BufferItems: defaultBufferItems,
		},
	)
	if err != nil {
		return err
	}
	l.idCache = ic
	return nil
}

// numCounters calculates the recommended NumCounters setting for the Kind
// cache, which is 10x the number of entries that will fit in the cache.
//
// Ref: https://github.com/dgraph-io/ristretto/blob/402101df6c698ed1253bb305ce9cda71bc83ad1d/cache.go#L93-L94
func numCounters(maxSize int, entrySize uintptr) int64 {
	return int64((maxSize / int(entrySize)) * 10)
}
