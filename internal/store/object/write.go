package store

import (
	"context"

	"github.com/relexec/rxp/object"

	storedomain "github.com/relexec/rxp-pg/internal/store/domain"
	storekind "github.com/relexec/rxp-pg/internal/store/kind"
	storekindversion "github.com/relexec/rxp-pg/internal/store/kindversion"
	storesystem "github.com/relexec/rxp-pg/internal/store/system"
)

// Write atomically writes the supplied Object to persistent storage. On
// successful write, the newly created or updated Object is returned.
func (s *Store) Write(
	ctx context.Context,
	sysRec storesystem.Record,
	kindRec storekind.Record,
	kvRec storekindversion.Record,
	domRec *storedomain.Record,
	obj object.Object,
) (*object.Object, error) {
	expectGeneration := obj.Generation()
	if expectGeneration == 0 {
		// caller expects that they are the first writer of this object. This
		// means we can attempt to insert into the objects table with this
		// object's keys and a generation of 1. any returned unique key
		// contraint violation will indicate another caller tried to create the
		// exact same object concurrently.
		return s.dbInsertFirst(
			ctx, sysRec, kindRec, kvRec, domRec, obj,
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
		sysRec, kindRec, kvRec, domRec,
		obj, expectGeneration,
	)
}
