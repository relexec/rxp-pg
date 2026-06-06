package store

import (
	"github.com/relexec/rxp-pg/internal/store"
	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storekindversion "github.com/relexec/rxp-pg/internal/store/kindversion"
	storenamespace "github.com/relexec/rxp-pg/internal/store/namespace"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Store facilitates reading and writing Object data.
type Store struct {
	store.Store

	// hostSystemRecord is the host System managed by the Driver.
	hostSystemRecord storesystem.Record
	// systemStore contains the Store for reading and writing System data.
	systemStore *storesystem.Store

	// kindStore contains the Store for reading and writing Kind data.
	kindStore *storekind.Store
	// kindversionStore contains the Store for reading and writing KindVersion
	// data.
	kindversionStore *storekindversion.Store

	// domainStore contains the Store for reading and writing Domain data.
	domainStore *storedomain.Store
	// namespaceStore contains the Store for reading and writing Namespace
	// data.
	namespaceStore *storenamespace.Store
}
