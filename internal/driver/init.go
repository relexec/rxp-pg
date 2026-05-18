package driver

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/system"

	"github.com/relexec/rxp-pg/config"
	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storenamespace "github.com/relexec/rxp-pg/internal/store/namespace"
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

	if err = d.initDomainStore(ctx); err != nil {
		return err
	}

	if err = d.initNamespaceStore(ctx); err != nil {
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
					"Unable to determine rxp host system uuid",
				)
			}
			d.hostSystemUUID = v
		}
	}
	d.log.Info("host system uuid: %s", d.hostSystemUUID)
	if d.hostSystemName == "" {
		// try to find the system identifier by looking at the configuration
		// and environment variabled.
		if d.cfg.SystemName != "" {
			d.hostSystemName = d.cfg.SystemName
		} else {
			v := os.Getenv("RXP_SYSTEM_NAME")
			if v != "" {
				d.hostSystemName = v
			}
		}
	}
	if d.hostSystemName != "" {
		d.log.Info("host system name: %s", d.hostSystemName)
	}
	return nil
}

// initMetrics initializes the store's metrics handler.
func (d *Driver) initMetrics(ctx context.Context) error {
	d.log.V(4).Info("initializing metrics")
	if d.metrics == nil {
		metrics, err := metrics.New(ctx)
		if err != nil {
			return err
		}
		d.metrics = metrics
	}
	d.onClose = append(d.onClose, d.metrics.MeterProvider().Shutdown)
	err := metrics.Init(d.metrics)
	if err != nil {
		return fmt.Errorf("failed initializing metrics: %w", err)
	}
	d.log.Info("initialized metrics")
	return nil
}

// initDBPool initializates the pgx pool connectiond.
func (d *Driver) initDBPool(ctx context.Context) error {
	d.log.V(4).Info("initializing pgxpool connections")
	poolConfig, err := d.cfg.PGXPoolConfig()
	if err != nil {
		return err
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return fmt.Errorf("failed connecting to postgres: %w", err)
	}
	d.log.Info("initialized pgxpool connections")
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
			err = d.systemStore.Write(
				ctx,
				system.New(
					system.WithUUID(d.hostSystemUUID),
					system.WithName(d.hostSystemName),
				),
			)
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

// initNamespaceStore initializes the namespace store.
func (d *Driver) initNamespaceStore(ctx context.Context) error {
	d.log.V(4).Info("initializing namespace store")
	s, err := storenamespace.New(
		ctx,
		storenamespace.WithConfig(d.cfg),
		storenamespace.WithPool(d.pool),
		storenamespace.WithDomainStore(d.domainStore),
		storenamespace.WithHostSystemRecord(*d.hostSystemRecord),
	)
	if err != nil {
		return err
	}
	d.namespaceStore = s
	d.onClose = append(d.onClose, d.namespaceStore.Close)
	d.log.Info("initialized namespace store")
	return nil
}
