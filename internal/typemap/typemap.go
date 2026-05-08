package typemap

import (
	"context"

	"github.com/relexec/rxp-pg/config"
	"github.com/relexec/rxp-pg/internal/lookup/generationed"
	"github.com/relexec/rxp-pg/internal/lookup/simple"
)

// TypeMap manages a set of lookup maps of string Name <-> integer DB Row ID
// map entries.
type TypeMap struct {
	// cfg stores the TypeMap's configuration.
	cfg config.CacheConfig
	// kinds manages lookups for Kinds
	kinds *simple.Lookup
	// kindVersions manages lookups for KindVersions
	kindVersions *simple.Lookup
	// metas manages lookups for Metas
	metas *generationed.Lookup
	// domains manages lookups for Domains
	domains *simple.Lookup
	// namespaces manages lookups for Namespaces
	namespaces *simple.Lookup
}

// Kinds returns the Lookup for Kinds.
func (m TypeMap) Kinds() *simple.Lookup {
	return m.kinds
}

// KindVersions returns the Lookup for KindVersions.
func (m TypeMap) KindVersions() *simple.Lookup {
	return m.kindVersions
}

// Metas returns the Lookup for metas.
func (m TypeMap) Metas() *generationed.Lookup {
	return m.metas
}

// Domains returns the Lookup for Domains.
func (m TypeMap) Domains() *simple.Lookup {
	return m.domains
}

// Namespaces returns the Lookup for Namespaces.
func (m TypeMap) Namespaces() *simple.Lookup {
	return m.namespaces
}

// Close ensures the TypeMap drains properly.
func (m *TypeMap) Close(ctx context.Context) error {
	if m.kinds != nil {
		m.kinds.Close(ctx)
	}
	if m.kindVersions != nil {
		m.kindVersions.Close(ctx)
	}
	if m.metas != nil {
		m.metas.Close(ctx)
	}
	if m.domains != nil {
		m.domains.Close(ctx)
	}
	if m.namespaces != nil {
		m.namespaces.Close(ctx)
	}
	return nil
}

// init sets up the TypeMap, initializing the underlying reverse lookups.
func (m *TypeMap) init(ctx context.Context) error {
	kc, err := simple.New(
		ctx,
		simple.WithType("kind"),
		simple.WithConfig(m.cfg),
	)
	if err != nil {
		return err
	}
	m.kinds = kc
	kvc, err := simple.New(
		ctx,
		simple.WithType("kindversion"),
		simple.WithConfig(m.cfg),
	)
	if err != nil {
		return err
	}
	m.kindVersions = kvc
	mc, err := generationed.New(
		ctx,
		generationed.WithType("meta"),
		generationed.WithConfig(m.cfg),
	)
	if err != nil {
		return err
	}
	m.metas = mc
	dc, err := simple.New(
		ctx,
		simple.WithType("domain"),
		simple.WithConfig(m.cfg),
	)
	if err != nil {
		return err
	}
	m.domains = dc
	nc, err := simple.New(
		ctx,
		simple.WithType("namespace"),
		simple.WithConfig(m.cfg),
	)
	if err != nil {
		return err
	}
	m.namespaces = nc
	return nil
}
