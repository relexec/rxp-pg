package driver

import (
	"context"
	"errors"
	"slices"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/relexec/rxp/types"

	"github.com/relexec/rxp-pg/config"
	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storemeta "github.com/relexec/rxp-pg/internal/store/meta"
	storenamespace "github.com/relexec/rxp-pg/internal/store/namespace"
	storeobject "github.com/relexec/rxp-pg/internal/store/object"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Driver implements an rxp backend using PostgreSQL for persistence.
type Driver struct {
	// log is the top-level logger for the Driver.
	log *logr.Logger
	// cfg contains the configuration options for the Driver.
	cfg *config.Config
	// pool holds the underlying pgx connection pool. This connection pool is
	// shared by all Stores contained in the Driver.
	pool *pgxpool.Pool
	// metrics is the metrics handler for the Driver.
	metrics types.Metrics

	// hostSystemUUID is the UUID of the host System managed by this Driver.
	hostSystemUUID string
	// hostSystemName is the name of the host System managed by this Driver, if
	// any.
	hostSystemName string
	// hostSystemRecord is the host System managed by this Driver.
	hostSystemRecord *storesystem.Record

	// systemStore contains the Store for reading and writing System data.
	systemStore *storesystem.Store
	// kindStore contains the Store for reading and writing Kind data.
	kindStore *storekind.Store
	// metaStore contains the Store for reading and writing Meta data.
	metaStore *storemeta.Store

	// domainStore contains the Store for reading and writing Domain data.
	domainStore *storedomain.Store
	// namespaceStore contains the Store for reading and writing Namespace
	// data.
	namespaceStore *storenamespace.Store

	// objectStore contains the Store for reading and writing Namespace
	// data.
	objectStore *storeobject.Store

	// onClose are a set of callbacks that will be executed in reverse
	// order when the Driver is closed.
	onClose []func(context.Context) error
}

// Metrics returns the Driver's configured metrics handler.
func (d *Driver) Metrics() types.Metrics {
	return d.metrics
}

// Close tears down the Driver and executes any callbacks that were registered
// to execute on shutdown.
func (d *Driver) Close(ctx context.Context) error {
	if d.pool != nil {
		d.pool.Close()
	}
	var err error
	slices.Reverse(d.onClose)
	for _, cb := range d.onClose {
		err = errors.Join(err, cb(ctx))
	}
	return err
}
