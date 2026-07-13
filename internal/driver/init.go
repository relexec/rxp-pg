package driver

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/relexec/rxp/api/metrics"
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/system"

	"github.com/relexec/rxp-pg/config"
	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storekindversion "github.com/relexec/rxp-pg/internal/store/kindversion"
	storeobject "github.com/relexec/rxp-pg/internal/store/object"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

func (d *Driver) init(ctx context.Context) error {
	if d.cfg == nil {
		d.cfg = config.Default()
	}
	if d.log == nil {
		lc := logr.FromContextOrDiscard(ctx)
		d.log = &lc
	}
	d.log.WithName("rxp.pg.store")

	err := d.cfg.Validate()
	if err != nil {
		return err
	}

	if err = d.ensureHostSystem(); err != nil {
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
		if d.cfg.SystemUUID != "" {
			d.hostSystemUUID = d.cfg.SystemUUID
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
	d.log.Info("host system uuid: %s", d.hostSystemUUID)
	if d.hostSystemTag == "" {
		// try to find the system identifier by looking at the configuration
		// and environment variabled.
		if d.cfg.SystemTag != "" {
			d.hostSystemTag = d.cfg.SystemTag
		} else {
			v := os.Getenv("RXP_SYSTEM_TAG")
			if v != "" {
				d.hostSystemTag = v
			}
		}
	}
	if d.hostSystemTag != "" {
		d.log.Info("host system name: %s", d.hostSystemTag)
	}
	return nil
}

// initMetrics initializes the store's metrics handler.
func (d *Driver) initMetrics(ctx context.Context) error {
	d.log.V(4).Info("initializing metrics")
	if d.metrics == nil {
		h, err := metrics.New(ctx)
		if err != nil {
			return fmt.Errorf("failed initializing metrics: %w", err)
		}
		d.metrics = h
	}
	d.onClose = append(d.onClose, d.metrics.MeterProvider().Shutdown)
	d.log.Info("initialized metrics")
	return nil
}

// initDBPool initializates the pgx pool connectiond.
func (d *Driver) initDBPool(ctx context.Context) error {
	d.log.V(4).Info("initializing db connection pool")
	poolConfig, err := d.cfg.PGXPoolConfig()
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
	d.log.Info("initialized db connection pool")
	d.pool = pool
	return nil
}

// initSystemStore initializes the system store.
func (d *Driver) initSystemStore(ctx context.Context) error {
	d.log.V(4).Info("initializing system store")
	s, err := storesystem.New(
		ctx,
		storesystem.WithConfig(d.cfg),
		storesystem.WithPool(d.pool),
	)
	if err != nil {
		return err
	}
	d.systemStore = s
	d.onClose = append(d.onClose, d.systemStore.Close)
	d.log.Info("initialized system store")
	return nil
}

// initKindStore initializes the kind store.
func (d *Driver) initKindStore(ctx context.Context) error {
	d.log.V(4).Info("initializing kind store")
	s, err := storekind.New(
		ctx,
		storekind.WithConfig(d.cfg),
		storekind.WithPool(d.pool),
		storekind.WithSystemStore(d.systemStore),
		storekind.WithHostSystemRecord(*d.hostSystemRecord),
	)
	if err != nil {
		return err
	}
	d.kindStore = s
	d.onClose = append(d.onClose, d.kindStore.Close)
	d.log.Info("initialized kind store")
	return nil
}

// initKindVersionStore initializes the kindversion store.
func (d *Driver) initKindVersionStore(ctx context.Context) error {
	d.log.V(4).Info("initializing kindversion store")
	s, err := storekindversion.New(
		ctx,
		storekindversion.WithConfig(d.cfg),
		storekindversion.WithPool(d.pool),
		storekindversion.WithSystemStore(d.systemStore),
		storekindversion.WithHostSystemRecord(*d.hostSystemRecord),
		storekindversion.WithKindStore(d.kindStore),
	)
	if err != nil {
		return err
	}
	d.kindversionStore = s
	d.onClose = append(d.onClose, d.kindversionStore.Close)
	d.log.Info("initialized kindversion store")
	return nil
}

// initHostSystemRecord ensures that we have our System record for the host
// system available.
func (d *Driver) initHostSystemRecord(ctx context.Context) error {
	d.log.V(4).Info("initializing host system record")
	if d.hostSystemRecord == nil {
		rec, err := d.systemStore.ReadByUUID(ctx, d.hostSystemUUID)
		if err != nil {
			if err != errors.ErrNotFound {
				return err
			}
			d.log.V(4).Info("creating host system record")
			initCtx := rxpcontext.SetIdentity(ctx, "rxp.system")
			sys := system.New(
				system.WithUUID(d.hostSystemUUID),
				system.WithTag(d.hostSystemTag),
			)
			err = d.systemStore.Write(initCtx, *sys)
			if err != nil {
				return err
			}
			d.log.Info("created host system record")
			rec, err = d.systemStore.ReadByUUID(ctx, d.hostSystemUUID)
			if err != nil {
				return err
			}
		}
		d.hostSystemRecord = rec
	}
	d.log.Info("host system record initialized")
	return nil
}

// initDomainStore initializes the domain store.
func (d *Driver) initDomainStore(ctx context.Context) error {
	d.log.V(4).Info("initializing domain store")
	s, err := storedomain.New(
		ctx,
		storedomain.WithConfig(d.cfg),
		storedomain.WithPool(d.pool),
		storedomain.WithSystemStore(d.systemStore),
		storedomain.WithHostSystemRecord(*d.hostSystemRecord),
	)
	if err != nil {
		return err
	}
	d.domainStore = s
	d.onClose = append(d.onClose, d.domainStore.Close)
	d.log.Info("initialized domain store")
	return nil
}

// initObjectStore initializes the object store.
func (d *Driver) initObjectStore(ctx context.Context) error {
	d.log.V(4).Info("initializing object store")
	s, err := storeobject.New(
		ctx,
		storeobject.WithConfig(d.cfg),
		storeobject.WithPool(d.pool),
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
	d.log.Info("initialized object store")
	return nil
}
