package store

import (
	"context"

	"github.com/relexec/rxp/api"
	"github.com/relexec/rxp/object"

	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storekindversion "github.com/relexec/rxp-pg/internal/store/kindversion"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Record decorates an Object with internal DB information.
type Record struct {
	// RowID is the internal database SERIAL for the objects record.
	RowID int64
	// Object is the publicly-exposed Object object.
	Object *object.Object
}

// NameQualifier contains either a System or Domain store record that qualifies
// an Object name.
type NameQualifier struct {
	System storesystem.Record
	Domain *storedomain.Record
}

// UUIDFromName returns the UUID associated with the supplied object name with
// either a System or Domain record.
func (s *Store) UUIDFromName(
	ctx context.Context,
	name string,
	qualifier NameQualifier,
) (string, error) {
	if qualifier.Domain != nil {
		return s.dbUUIDFromNameDomainQualified(
			ctx, qualifier.System, *qualifier.Domain, name,
		)
	}
	return s.dbUUIDFromNameSystemQualified(ctx, qualifier.System, name)
}

// ReadByRowID returns a Record for the Object with the supplied internal DB
// row ID and generation.
func (s *Store) ReadByRowIDAndGeneration(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kvRec storekindversion.Record,
	domRec *storedomain.Record,
	rowID int64,
	requestedGen api.Generation,
) (*Record, error) {
	if kindRec.Kind.Scope() == api.ScopeDomain {
		return s.dbReadByRowIDAndGenerationDomainQualified(
			ctx, sysRec, kindRec, kvRec, *domRec, rowID, requestedGen,
		)
	}
	return s.dbReadByRowIDAndGenerationSystemQualified(
		ctx, sysRec, kindRec, kvRec, rowID, requestedGen,
	)
}

// ReadByUUIDAndGeneration returns a Record for the Object with the supplied
// UUID amd generation.
func (s *Store) ReadByUUIDAndGeneration(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kvRec storekindversion.Record,
	domRec *storedomain.Record,
	uuid string,
	requestedGen api.Generation,
) (*Record, error) {
	scope := kindRec.Kind.Scope()
	if scope == api.ScopeDomain {
		return s.dbReadByUUIDAndGenerationDomainQualified(
			ctx, sysRec, kindRec, kvRec, domRec, uuid, requestedGen,
		)
	}
	return s.dbReadByUUIDAndGenerationSystemQualified(
		ctx, sysRec, kindRec, kvRec, uuid, requestedGen,
	)
}

// ReadByNameAndGeneration returns a Record for the Object with the supplied
// Name and generation.
func (s *Store) ReadByNameAndGeneration(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kvRec storekindversion.Record,
	domRec *storedomain.Record,
	name string,
	requestedGen api.Generation,
) (*Record, error) {
	if kindRec.Kind.Scope() == api.ScopeDomain {
		return s.dbReadByNameAndGenerationDomainQualified(
			ctx, sysRec, kindRec, kvRec, *domRec, name, requestedGen,
		)
	}
	return s.dbReadByNameAndGenerationSystemQualified(
		ctx, sysRec, kindRec, kvRec, name, requestedGen,
	)
}
