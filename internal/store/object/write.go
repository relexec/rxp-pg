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

// WriteFirst atomically writes the pre-validated Object to persistent storage.
// This method is called when the caller believes they are the first writer of
// an object.
func (s *Store) WriteFirst(
	ctx context.Context,
	obj *object.Object,
) error {
	sys := obj.System()
	sysRec, err := s.systemStore.ReadByUUID(ctx, sys.UUID())
	if err != nil {
		return errors.Internal(
			"failed reading system record",
			errors.WithWrap(err),
		)
	}
	kv := obj.KindVersion()
	kindRec, err := s.kindStore.ReadByName(ctx, sys, kv.Kind())
	if err != nil {
		if err == errors.ErrNotFound {
			return errors.ErrKindVersionUnknown
		}
		return errors.Internal(
			"failed reading kind record",
			errors.WithWrap(err),
		)
	}
	metaRec, err := s.metaStore.ReadByKindVersion(
		ctx, sys, kv,
	)
	if err != nil {
		if err == errors.ErrNotFound {
			return errors.ErrKindVersionUnknown
		}
		return errors.Internal(
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
			return errors.Internal(
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
			return errors.Internal(
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
			return errors.Internal(
				"failed reading domain record",
				errors.WithWrap(err),
			)
		}
		nsRec, err = s.namespaceStore.ReadByName(
			ctx, dom, ns.Name(),
		)
		if err != nil {
			return errors.Internal(
				"failed reading namespace record",
				errors.WithWrap(err),
			)
		}
	case api.ScopeDomain:
		dom := obj.Domain()
		if dom == nil {
			return errors.Internal(
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
	}
	return s.dbInsertFirst(ctx, sysRec, kindRec, metaRec, domRec, nsRec, obj)
}

// WriteGeneration atomically writes the pre-validated Object to persistent
// storage.  This method is called when the caller believes they are the NOT
// thefirst writer of an object and expect to find a supplied latest
// generation.
func (s *Store) WriteGeneration(
	ctx context.Context,
	obj *object.Object,
	expectGeneration api.Generation,
) error {
	sys := obj.System()
	sysRec, err := s.systemStore.ReadByUUID(ctx, sys.UUID())
	if err != nil {
		return errors.Internal(
			"failed reading system record",
			errors.WithWrap(err),
		)
	}
	kv := obj.KindVersion()
	kindRec, err := s.kindStore.ReadByName(ctx, sys, kv.Kind())
	if err != nil {
		if err == errors.ErrNotFound {
			return errors.ErrKindVersionUnknown
		}
		return errors.Internal(
			"failed reading kind record",
			errors.WithWrap(err),
		)
	}
	metaRec, err := s.metaStore.ReadByKindVersion(
		ctx, sys, kv,
	)
	if err != nil {
		if err == errors.ErrNotFound {
			return errors.ErrKindVersionUnknown
		}
		return errors.Internal(
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
			return errors.Internal(
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
			return errors.Internal(
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
			return errors.Internal(
				"failed reading domain record",
				errors.WithWrap(err),
			)
		}
		nsRec, err = s.namespaceStore.ReadByName(
			ctx, dom, ns.Name(),
		)
		if err != nil {
			return errors.Internal(
				"failed reading namespace record",
				errors.WithWrap(err),
			)
		}
	case api.ScopeDomain:
		dom := obj.Domain()
		if dom == nil {
			return errors.Internal(
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
	}
	return s.dbInsertGeneration(ctx, sysRec, kindRec, metaRec, domRec, nsRec, obj, expectGeneration)
}
