package driver

import (
	"context"
	"errors"
	"log/slog"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/relexec/rxp/api/metrics"

	"github.com/relexec/rxp-pg/config"
	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storekindversion "github.com/relexec/rxp-pg/internal/store/kindversion"
	storeobject "github.com/relexec/rxp-pg/internal/store/object"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Driver implements an rxp backend using PostgreSQL for persistence.
type Driver struct {
	// Logger is the top-level logger for the Driver.
	Logger *slog.Logger
	// Config contains the configuration options for the Driver.
	Config config.Config
	// pool holds the underlying pgx connection pool. This connection pool is
	// shared by all Stores contained in the Driver.
	Pool *pgxpool.Pool

	// Metrics is the metrics handler for the Driver.
	Metrics *metrics.Handler

	// hostSystemUUID is the UUID of the host System managed by this Driver.
	hostSystemUUID string
	// hostSystemTag is the tag of the host System managed by this Driver, if
	// any.
	hostSystemTag string
	// hostSystemRecord is the host System managed by this Driver.
	hostSystemRecord *storesystem.Record

	// systemStore contains the Store for reading and writing System data.
	systemStore *storesystem.Store
	// kindStore contains the Store for reading and writing Kind data.
	kindStore *storekind.Store
	// kindversionStore contains the Store for reading and writing KindVersion
	// data.
	kindversionStore *storekindversion.Store
	// domainStore contains the Store for reading and writing Domain data.
	domainStore *storedomain.Store
	// objectStore contains the Store for reading and writing Object data.
	objectStore *storeobject.Store

	// onClose are a set of callbacks that will be executed in reverse
	// order when the Driver is closed.
	onClose []func(context.Context) error
}

// Close tears down the Driver and executes any callbacks that were registered
// to execute on shutdown.
func (d *Driver) Close(ctx context.Context) error {
	if d.Pool != nil {
		d.Pool.Close()
	}
	var err error
	slices.Reverse(d.onClose)
	for _, cb := range d.onClose {
		err = errors.Join(err, cb(ctx))
	}
	return err
}
