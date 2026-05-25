package store

import (
	"context"
	"fmt"

	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storenamespace "github.com/relexec/rxp-pg/internal/store/namespace"
	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/object"
)

// Write atomically writes the supplied Object to persistent storage. On
// successful write, the newly created or updated Object is returned.
func (s *Store) Write(
	ctx context.Context,
	obj object.Object,
) (*object.Object, error) {
	sys := obj.System()
	sysRec, err := s.systemStore.ReadByUUID(ctx, sys.UUID())
	if err != nil {
		return nil, errors.Internal(
			"failed reading system record",
			errors.WithWrap(err),
		)
	}
	kv := obj.KindVersion()
	kindRec, err := s.kindStore.ReadByName(ctx, sys, kv.Kind())
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
		ctx, sys, kv,
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

	var domRec *storedomain.Record
	var nsRec *storenamespace.Record

	k := kindRec.Kind
	scope := k.Scope()
	switch scope {
	case api.ScopeNamespace:
		ns := obj.Namespace()
		if ns == nil {
			return nil, errors.Internal(
				fmt.Sprintf(
					"expected to have namespace for object %q",
					obj.UUID(),
				),
			)
		}
		dom := obj.Domain()
		if dom == nil {
			dom = ns.Domain()
		}
		if dom == nil {
			return nil, errors.Internal(
				fmt.Sprintf(
					"expected to have domain for namespace %q",
					ns.Name(),
				),
			)
		}
		if dom.UUID() != "" {
			domRec, err = s.domainStore.ReadByUUID(
				ctx, dom.UUID(),
			)
		} else {
			domRec, err = s.domainStore.ReadByName(
				ctx, sys, dom.Name(),
			)
		}
		if err != nil {
			return nil, errors.Internal(
				"failed reading domain record",
				errors.WithWrap(err),
			)
		}
		nsRec, err = s.namespaceStore.ReadByName(
			ctx, dom, ns.Name(),
		)
		if err != nil {
			return nil, errors.Internal(
				"failed reading namespace record",
				errors.WithWrap(err),
			)
		}
	case api.ScopeDomain:
		dom := obj.Domain()
		if dom == nil {
			return nil, errors.Internal(
				fmt.Sprintf(
					"expected to have domain for object %q",
					obj.UUID(),
				),
			)
		}
		if dom.UUID() != "" {
			domRec, err = s.domainStore.ReadByUUID(
				ctx, dom.UUID(),
			)
		} else {
			domRec, err = s.domainStore.ReadByName(
				ctx, sys, dom.Name(),
			)
		}
		if err != nil {
			return nil, errors.Internal(
				"failed reading domain record",
				errors.WithWrap(err),
			)
		}
	}
	expectGeneration := obj.Generation()
	if expectGeneration == 0 {
		// caller expects that they are the first writer of this object. This
		// means we can attempt to insert into the objects table with this
		// object's keys and a generation of 1. any returned unique key
		// contraint violation will indicate another caller tried to create the
		// exact same object concurrently.
		return s.dbInsertFirst(
			ctx, sysRec, kindRec, metaRec, domRec, nsRec, obj,
		)
	}
	// Otherwise, the caller expects that there is an existing object with this
	// object's keys and that the latest generation of said object matches a
	// supplied generation marker. In this case, we insert a new record into
	// the object_generations table and update the objects table using a WHERE
	// condition against the expected generation. If this UPDATE fails to
	// return any affected rows, we know another caller beat us to write their
	// updated desired state changes and we need to either retry the write or
	// fail.
	return s.dbInsertGeneration(
		ctx,
		sysRec, kindRec, metaRec, domRec, nsRec,
		obj, expectGeneration,
	)
}
