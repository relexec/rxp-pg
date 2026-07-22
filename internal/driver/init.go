package driver

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/api/metrics"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/system"

	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storekindversion "github.com/relexec/rxp-pg/internal/store/kindversion"
	storeobject "github.com/relexec/rxp-pg/internal/store/object"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

func (d *Driver) init(ctx context.Context) error {
	err := d.ensureHostSystem()
	if err != nil {
		return err
	}

	if err = d.initMetrics(ctx); err != nil {
		return err
	}

	if err = d.initDBPool(ctx); err != nil {
		return err
	}

	if err = d.initSystemStore(ctx); err != nil {
		return err
	}

	if err = d.initHostSystemRecord(ctx); err != nil {
		return err
	}

	if err = d.initKindStore(ctx); err != nil {
		return err
	}

	if err = d.initKindVersionStore(ctx); err != nil {
		return err
	}

	if err = d.initDomainStore(ctx); err != nil {
		return err
	}

	if err = d.initObjectStore(ctx); err != nil {
		return err
	}
	return nil
}

// ensureHostSystem ensures that we know our host system identifier and
// optional host system name.
func (d *Driver) ensureHostSystem() error {
	if d.hostSystemUUID == "" {
		// try to find the system identifier by looking at the configuration
		// and environment variabled.
		if d.Config.SystemUUID != "" {
			d.hostSystemUUID = d.Config.SystemUUID
		} else {
			v := os.Getenv("RXP_SYSTEM_UUID")
			if v == "" {
				return fmt.Errorf(
					"failed determining rxp host system uuid",
				)
			}
			d.hostSystemUUID = v
		}
	}
	d.Logger.Info(fmt.Sprintf("host system uuid: %s", d.hostSystemUUID))
	if d.hostSystemTag == "" {
		// try to find the system identifier by looking at the configuration
		// and environment variabled.
		if d.Config.SystemTag != "" {
			d.hostSystemTag = d.Config.SystemTag
		} else {
			v := os.Getenv("RXP_SYSTEM_TAG")
			if v != "" {
				d.hostSystemTag = v
			}
		}
	}
	if d.hostSystemTag != "" {
		d.Logger.Info(fmt.Sprintf("host system name: %s", d.hostSystemTag))
	}
	return nil
}

// initMetrics initializes the store's metrics handler.
func (d *Driver) initMetrics(ctx context.Context) error {
	d.Logger.Debug("initializing metrics")
	if d.Metrics == nil {
		h, err := metrics.New(ctx)
		if err != nil {
			return fmt.Errorf("failed initializing metrics: %w", err)
		}
		d.Metrics = h
	}
	d.onClose = append(d.onClose, d.Metrics.MeterProvider().Shutdown)
	d.Logger.Info("initialized metrics")
	return nil
}

// initDBPool initializates the pgx pool connectiond.
func (d *Driver) initDBPool(ctx context.Context) error {
	d.Logger.Debug("initializing db connection pool")
	poolConfig, err := d.Config.PGXPoolConfig()
	if err != nil {
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("failed creating db connection pool: %w", err)
	}
	if err = pool.Ping(ctx); err != nil {
		return fmt.Errorf("failed pinging db: %w", err)
	}
	d.Logger.Info("initialized db connection pool")
	d.Pool = pool
	return nil
}

// initSystemStore initializes the system store.
func (d *Driver) initSystemStore(ctx context.Context) error {
	d.Logger.Debug("initializing system store")
	s, err := storesystem.New(
		ctx, d.Config, d.Pool,
	)
	if err != nil {
		return err
	}
	d.systemStore = s
	d.onClose = append(d.onClose, d.systemStore.Close)
	d.Logger.Info("initialized system store")
	return nil
}

// initKindStore initializes the kind store.
func (d *Driver) initKindStore(ctx context.Context) error {
	d.Logger.Debug("initializing kind store")
	s, err := storekind.New(
		ctx, d.Config, d.Pool,
		storekind.WithSystemStore(d.systemStore),
		storekind.WithHostSystemRecord(*d.hostSystemRecord),
	)
	if err != nil {
		return err
	}
	d.kindStore = s
	d.onClose = append(d.onClose, d.kindStore.Close)
	d.Logger.Info("initialized kind store")
	return nil
}

// initKindVersionStore initializes the kindversion store.
func (d *Driver) initKindVersionStore(ctx context.Context) error {
	d.Logger.Debug("initializing kindversion store")
	s, err := storekindversion.New(
		ctx, d.Config, d.Pool,
		storekindversion.WithSystemStore(d.systemStore),
		storekindversion.WithHostSystemRecord(*d.hostSystemRecord),
		storekindversion.WithKindStore(d.kindStore),
	)
	if err != nil {
		return err
	}
	d.kindversionStore = s
	d.onClose = append(d.onClose, d.kindversionStore.Close)
	d.Logger.Info("initialized kindversion store")
	return nil
}

// initHostSystemRecord ensures that we have our System record for the host
// system available.
func (d *Driver) initHostSystemRecord(ctx context.Context) error {
	d.Logger.Debug("initializing host system record")
	if d.hostSystemRecord == nil {
		rec, err := d.systemStore.ReadByUUID(ctx, d.hostSystemUUID)
		if err != nil {
			if err != errors.ErrNotFound {
				return err
			}
			d.Logger.Debug("creating host system record")
			initCaller := api.Caller{Identity: "rxp.system"}
			initCtx := api.CallerToContext(ctx, initCaller)
			sys := system.New(
				system.WithUUID(d.hostSystemUUID),
				system.WithTag(d.hostSystemTag),
			)
			err = d.systemStore.Write(initCtx, *sys)
			if err != nil {
				return err
			}
			d.Logger.Info("created host system record")
			rec, err = d.systemStore.ReadByUUID(ctx, d.hostSystemUUID)
			if err != nil {
				return err
			}
		}
		d.hostSystemRecord = rec
	}
	d.Logger.Info("host system record initialized")
	return nil
}

// initDomainStore initializes the domain store.
func (d *Driver) initDomainStore(ctx context.Context) error {
	d.Logger.Debug("initializing domain store")
	s, err := storedomain.New(
		ctx, d.Config, d.Pool,
		storedomain.WithSystemStore(d.systemStore),
		storedomain.WithHostSystemRecord(*d.hostSystemRecord),
	)
	if err != nil {
		return err
	}
	d.domainStore = s
	d.onClose = append(d.onClose, d.domainStore.Close)
	d.Logger.Info("initialized domain store")
	return nil
}

// initObjectStore initializes the object store.
func (d *Driver) initObjectStore(ctx context.Context) error {
	d.Logger.Debug("initializing object store")
	s, err := storeobject.New(
		ctx, d.Config, d.Pool,
		storeobject.WithHostSystemRecord(*d.hostSystemRecord),
		storeobject.WithSystemStore(d.systemStore),
		storeobject.WithKindStore(d.kindStore),
		storeobject.WithKindVersionStore(d.kindversionStore),
		storeobject.WithDomainStore(d.domainStore),
	)
	if err != nil {
		return err
	}
	d.objectStore = s
	d.onClose = append(d.onClose, d.objectStore.Close)
	d.Logger.Info("initialized object store")
	return nil
}
