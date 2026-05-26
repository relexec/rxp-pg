package store

import (
	"context"

	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/domain"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/kind"
	"github.com/relexec/rxp/meta"
	"github.com/relexec/rxp/namespace"
	"github.com/relexec/rxp/object"
	"github.com/relexec/rxp/system"
)

// Record decorates an Object with internal DB information.
type Record struct {
	// RowID is the internal database SERIAL for the objects record.
	RowID int64
	// Object is the publicly-exposed Object object.
	Object *object.Object
}

// ReadByRowID returns a Record for the Object with the supplied internal DB
// row ID.
func (s *Store) ReadByRowID(
	ctx context.Context,
	sys *system.System,
	k *kind.Kind,
	m *meta.Meta,
	dom *domain.Domain,
	ns *namespace.Namespace,
	rowID int64,
) (*Record, error) {
	sysRec, err := s.systemStore.ReadByUUID(ctx, sys.UUID())
	if err != nil {
		return nil, errors.Internal(
			"failed reading system record",
			errors.WithWrap(err),
		)
	}
	kindRec, err := s.kindStore.ReadByName(ctx, sys, k.Name())
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrKindVersionUnknown
		}
		return nil, errors.Internal(
			"failed reading kind record",
			errors.WithWrap(err),
		)
	}
	metaRec, err := s.metaStore.ReadByKindVersion(
		ctx, sys, m.KindVersion(),
	)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrKindVersionUnknown
		}
		return nil, errors.Internal(
			"failed reading meta record",
			errors.WithWrap(err),
		)
	}
	scope := k.Scope()
	switch scope {
	case api.ScopeNamespace:
		if ns == nil {
			return nil, errors.ErrSelectorNamespaceRequired
		}
		if ns.Domain() == nil {
			return nil, errors.ErrSelectorDomainRequired
		}
		nsRec, err := s.namespaceStore.ReadByName(
			ctx, ns.Domain(), ns.Name(),
		)
		if err != nil {
			if err == errors.ErrNotFound {
				return nil, err
			}
			return nil, errors.Internal(
				"failed reading namespace record",
				errors.WithWrap(err),
			)
		}
		return s.dbReadNamespaceQualifiedByRowID(
			ctx, sysRec, kindRec, metaRec, nsRec, rowID,
		)
	case api.ScopeDomain:
		if dom == nil {
			return nil, errors.ErrSelectorDomainRequired
		}
		var domRec *storedomain.Record
		if dom.UUID() != "" {
			domRec, err = s.domainStore.ReadByUUID(
				ctx, dom.UUID(),
			)
		} else {
			domRec, err = s.domainStore.ReadByName(
				ctx, sys, dom.Name(),
			)
		}
		return s.dbReadDomainQualifiedByRowID(
			ctx, sysRec, kindRec, metaRec, domRec, rowID,
		)
	default:
		return s.dbReadSystemQualifiedByRowID(
			ctx, sysRec, kindRec, metaRec, rowID,
		)
	}
}

// ReadByUUID returns a Record for the Object with the supplied UUID.
func (s *Store) ReadByUUID(
	ctx context.Context,
	sys *system.System,
	k *kind.Kind,
	m *meta.Meta,
	dom *domain.Domain,
	ns *namespace.Namespace,
	uuid string,
) (*Record, error) {
	sysRec, err := s.systemStore.ReadByUUID(ctx, sys.UUID())
	if err != nil {
		return nil, errors.Internal(
			"failed reading system record",
			errors.WithWrap(err),
		)
	}
	kindRec, err := s.kindStore.ReadByName(ctx, sys, k.Name())
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrKindVersionUnknown
		}
		return nil, errors.Internal(
			"failed reading kind record",
			errors.WithWrap(err),
		)
	}
	metaRec, err := s.metaStore.ReadByKindVersion(
		ctx, sys, m.KindVersion(),
	)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrKindVersionUnknown
		}
		return nil, errors.Internal(
			"failed reading meta record",
			errors.WithWrap(err),
		)
	}
	scope := k.Scope()
	switch scope {
	case api.ScopeNamespace:
		if ns == nil {
			return nil, errors.ErrSelectorNamespaceRequired
		}
		if ns.Domain() == nil {
			return nil, errors.ErrSelectorDomainRequired
		}
		nsRec, err := s.namespaceStore.ReadByName(
			ctx, ns.Domain(), ns.Name(),
		)
		if err != nil {
			if err == errors.ErrNotFound {
				return nil, err
			}
			return nil, errors.Internal(
				"failed reading namespace record",
				errors.WithWrap(err),
			)
		}
		return s.dbReadNamespaceQualifiedByUUID(
			ctx, sysRec, kindRec, metaRec, nsRec, uuid,
		)
	case api.ScopeDomain:
		if dom == nil {
			return nil, errors.ErrSelectorDomainRequired
		}
		var domRec *storedomain.Record
		if dom.UUID() != "" {
			domRec, err = s.domainStore.ReadByUUID(
				ctx, dom.UUID(),
			)
		} else {
			domRec, err = s.domainStore.ReadByName(
				ctx, sys, dom.Name(),
			)
		}
		return s.dbReadDomainQualifiedByUUID(
			ctx, sysRec, kindRec, metaRec, domRec, uuid,
		)
	default:
		return s.dbReadSystemQualifiedByUUID(
			ctx, sysRec, kindRec, metaRec, uuid,
		)
	}
}

// ReadByName returns a Record for the Object with the supplied Name.
func (s *Store) ReadByName(
	ctx context.Context,
	sys *system.System,
	k *kind.Kind,
	m *meta.Meta,
	dom *domain.Domain,
	ns *namespace.Namespace,
	name string,
) (*Record, error) {
	sysRec, err := s.systemStore.ReadByUUID(ctx, sys.UUID())
	if err != nil {
		return nil, errors.Internal(
			"failed reading system record",
			errors.WithWrap(err),
		)
	}
	kindRec, err := s.kindStore.ReadByName(ctx, sys, k.Name())
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrKindVersionUnknown
		}
		return nil, errors.Internal(
			"failed reading kind record",
			errors.WithWrap(err),
		)
	}
	metaRec, err := s.metaStore.ReadByKindVersion(
		ctx, sys, m.KindVersion(),
	)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrKindVersionUnknown
		}
		return nil, errors.Internal(
			"failed reading meta record",
			errors.WithWrap(err),
		)
	}
	scope := k.Scope()
	switch scope {
	case api.ScopeNamespace:
		if ns == nil {
			return nil, errors.ErrSelectorNamespaceRequired
		}
		if ns.Domain() == nil {
			return nil, errors.ErrSelectorDomainRequired
		}
		nsRec, err := s.namespaceStore.ReadByName(
			ctx, ns.Domain(), ns.Name(),
		)
		if err != nil {
			if err == errors.ErrNotFound {
				return nil, err
			}
			return nil, errors.Internal(
				"failed reading namespace record",
				errors.WithWrap(err),
			)
		}
		return s.dbReadNamespaceQualifiedByName(
			ctx, sysRec, kindRec, metaRec, nsRec, name,
		)
	case api.ScopeDomain:
		if dom == nil {
			return nil, errors.ErrSelectorDomainRequired
		}
		var domRec *storedomain.Record
		if dom.UUID() != "" {
			domRec, err = s.domainStore.ReadByUUID(
				ctx, dom.UUID(),
			)
		} else {
			domRec, err = s.domainStore.ReadByName(
				ctx, sys, dom.Name(),
			)
		}
		return s.dbReadDomainQualifiedByName(
			ctx, sysRec, kindRec, metaRec, domRec, name,
		)
	default:
		return s.dbReadSystemQualifiedByName(
			ctx, sysRec, kindRec, metaRec, name,
		)
	}
}

// ReadAtGeneration grabs object data for a specified generation of the object
// desired state.
func (s *Store) ReadAtGeneration(
	ctx context.Context,
	rowID int64,
	generation api.Generation,
) (*Record, error) {
	return s.dbReadAtGeneration(ctx, rowID, generation)
}
