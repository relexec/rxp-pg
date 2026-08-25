package store

import (
	"context"

	"github.com/relexec/rxp/api"
	apicore "github.com/relexec/rxp/api/core"
	apidomain "github.com/relexec/rxp/api/domain"
	apiobject "github.com/relexec/rxp/api/object"
	apisystem "github.com/relexec/rxp/api/system"
)

// Record decorates an Object with internal DB information.
type Record struct {
	// RowID is the internal database SERIAL for the objects record.
	RowID int64
	// Object is the publicly-exposed Object object.
	Object *apiobject.Object
}

// NameQualifier contains either a System or Domain store record that qualifies
// an Object name.
type NameQualifier struct {
	System *apisystem.System
	Domain *apidomain.Domain
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

// NameFromUUID returns the Name associated with the supplied object name with
// either a System or Domain record.
func (s *Store) NameFromUUID(
	ctx context.Context,
	uuid string,
	qualifier NameQualifier,
) (string, error) {
	if qualifier.Domain != nil {
		return s.dbNameFromUUIDDomainQualified(
			ctx, qualifier.System, *qualifier.Domain, uuid,
		)
	}
	return s.dbNameFromUUIDSystemQualified(ctx, qualifier.System, uuid)
}

// ReadByRowID returns a Record for the Object with the supplied internal DB
// row ID and generation.
func (s *Store) ReadByRowIDAndGeneration(
	ctx context.Context,
	rowID int64,
	requestedGen apicore.Generation,
) (*Record, error) {
	return s.dbReadByRowIDAndGeneration(ctx, rowID, requestedGen)
}

// ReadByUUIDAndGeneration returns a Record for the Object with the supplied
// UUID amd generation.
func (s *Store) ReadByUUIDAndGeneration(
	ctx context.Context,
	kvRec *api.KindVersion,
	uuid string,
	requestedGen apicore.Generation,
) (*Record, error) {
	return s.dbReadByUUIDAndGeneration(ctx, kvRec, uuid, requestedGen)
}
