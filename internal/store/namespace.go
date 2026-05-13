package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	rxpcontext "github.com/relexec/rxp/context"
	"github.com/relexec/rxp/errors"
	"github.com/relexec/rxp/metrics"
	"github.com/relexec/rxp/namespace"
	readoption "github.com/relexec/rxp/namespace/read/option"
	"github.com/relexec/rxp/namespace/read/selector"
	writeoption "github.com/relexec/rxp/namespace/write/option"
	rxptypes "github.com/relexec/rxp/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type namespaceCacheKey string

func (k namespaceCacheKey) DomainName() rxptypes.DomainName {
	parts := strings.Split(string(k), "|")
	return rxptypes.DomainName(parts[0])
}

func (k namespaceCacheKey) NamespaceName() rxptypes.NamespaceName {
	parts := strings.Split(string(k), "|")
	return rxptypes.NamespaceName(parts[1])
}

func newNamespaceCacheKey(
	domain rxptypes.Domain,
	name rxptypes.NamespaceName,
) namespaceCacheKey {
	return namespaceCacheKey(string(domain.Name()) + "|" + string(name))
}

// namespaceEntry decorates a Namespace with internal DB information.
type namespaceEntry struct {
	// RowID is the internal database SERIAL for the namespaces record.
	RowID int64
	// DomainRowID is the internal database SERIAL for the namespace's associated
	// domain record.
	DomainRowID int64
	// Namespace is the publicly-exposed Namespace object.
	Namespace *namespace.Namespace
}

// NamespaceRead reads a Namespace from persistent storage.
//
// If the namespace cache is enabled, this method automatically caches the
// returned Namespace in the cache.
func (s *Store) NamespaceRead(
	ctx context.Context,
	sel selector.Selector,
	opts ...readoption.Option,
) (rxptypes.Namespace, error) {
	err := s.requestValidate(ctx)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeTargetType(metrics.TargetTypeNamespace),
		}
		if err != nil {
			attrs = append(attrs, metrics.AttributeErrCode(err))
		}
		metrics.InstrumentReadRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentReadDuration.Record(ctx, elapsed)
	}()

	ropts := readoption.New(opts...)
	err = s.namespaceReadValidate(ctx, sel, ropts)
	if err != nil {
		return nil, err
	}

	domain := sel.Domain()
	system := domain.System()
	// Default the system to the host system if it hasn't been specified.
	if system == nil {
		system = s.hostSystem.System
	}
	systemEntry, err := s.systemRead(ctx, system.UUID())
	if err != nil {
		return nil, err
	}

	domainEntry, err := s.domainRead(ctx, systemEntry, domain.Name())
	if err != nil {
		return nil, err
	}

	name := sel.Name()

	entry, err := s.namespaceRead(ctx, systemEntry, domainEntry, name)
	if err != nil {
		return nil, err
	}
	return entry.Namespace, nil
}

// NamespaceWrite atomically writes the supplied Namespace to persistent storage.
func (s *Store) NamespaceWrite(
	ctx context.Context,
	namespace rxptypes.Namespace,
	opts ...writeoption.Option,
) error {
	err := s.requestValidate(ctx)
	if err != nil {
		return err
	}
	start := time.Now()

	defer func() {
		elapsed := time.Since(start).Seconds()
		attrs := []attribute.KeyValue{
			metrics.AttributeTargetType(metrics.TargetTypeNamespace),
		}
		if err != nil {
			attrs = append(attrs, metrics.AttributeErrCode(err))
		}
		metrics.InstrumentWriteRequest.Add(
			ctx, 1,
			metric.WithAttributes(attrs...),
		)
		metrics.InstrumentWriteDuration.Record(ctx, elapsed)
	}()

	wopts := writeoption.New(opts...)
	err = s.namespaceWriteValidate(ctx, namespace, wopts)
	if err != nil {
		return err
	}

	domain := namespace.Domain()
	system := domain.System()
	// Default the system to the host system if it hasn't been specified.
	if system == nil {
		system = s.hostSystem.System
	}
	systemEntry, err := s.systemRead(ctx, system.UUID())
	if err != nil {
		return err
	}

	domainEntry, err := s.domainRead(ctx, systemEntry, domain.Name())
	if err != nil {
		return err
	}

	return s.namespaceDBWrite(ctx, domainEntry, namespace)
}

// namespaceReadValidate returns an error if the supplied selector and read
// options are not valid for reading a single Namespace.
func (s *Store) namespaceReadValidate(
	ctx context.Context,
	sel selector.Selector,
	opts readoption.Options,
) error {
	return sel.Validate()
}

// namespaceWriteValidate returns an error if the supplied namespace and write
// options are not valid for writing a single Namespace.
func (s *Store) namespaceWriteValidate(
	ctx context.Context,
	namespace rxptypes.Namespace,
	opts writeoption.Options,
) error {
	return namespace.Validate()
}

// namespaceRead returns a namespaceEntry for the supplied pre-validated
// domain and namespace name. This method will populate any caches with any
// read records.
func (s *Store) namespaceRead(
	ctx context.Context,
	systemEntry *systemEntry,
	domainEntry *domainEntry,
	name rxptypes.NamespaceName,
) (*namespaceEntry, error) {
	domain := domainEntry.Domain
	cacheKey := newNamespaceCacheKey(domain, name)
	cached, found := s.namespaceCacheRead(ctx, cacheKey)
	if found {
		return cached, nil
	}
	entry, err := s.namespaceDBRead(ctx, domainEntry, name)
	if err != nil {
		return nil, err
	}
	err = s.namespaceCacheWrite(ctx, cacheKey, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

// namespaceCacheRead returns a cached Namespace and whether or not the supplied
// namespace name was found in the cache.
func (s *Store) namespaceCacheRead(
	ctx context.Context,
	key namespaceCacheKey,
) (*namespaceEntry, bool) {
	if s.namespaceCache == nil {
		return nil, false
	}
	return s.namespaceCache.Get(key)
}

// namespaceCacheWrite writes the supplied cache entry if the namespace cache is
// enabled.
func (s *Store) namespaceCacheWrite(
	ctx context.Context,
	key namespaceCacheKey,
	entry *namespaceEntry,
) error {
	if s.namespaceCache == nil {
		return nil
	}
	set := s.namespaceCache.Set(key, entry)
	if !set {
		return errors.Internal(
			fmt.Sprintf("failed setting namespace cache key %q", key),
		)
	}
	return nil
}

// namespaceDBRead performs a SELECT query to return the stored namespace record.
func (s *Store) namespaceDBRead(
	ctx context.Context,
	domainEntry *domainEntry,
	name rxptypes.NamespaceName,
) (*namespaceEntry, error) {
	out := namespaceEntry{
		Namespace: namespace.New(
			namespace.WithDomain(domainEntry.Domain),
			namespace.WithName(name),
		),
		DomainRowID: domainEntry.RowID,
	}
	fn := func(tx pgx.Tx) error {
		qs := "SELECT id FROM namespaces WHERE domain = $1 AND name = $2"
		err := tx.QueryRow(ctx, qs, domainEntry.RowID, name).Scan(&out.RowID)
		if err != nil {
			if err == pgx.ErrNoRows {
				return errors.ErrNotFound
			}
			return errors.Internal(
				"failed reading namespaces record",
				errors.WithWrap(err),
			)
		}
		return nil
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return nil, err
	}
	return &out, nil
}

// namespaceDBWrite inserts the supplied namespace information into the database.
func (s *Store) namespaceDBWrite(
	ctx context.Context,
	domainEntry *domainEntry,
	namespace rxptypes.Namespace,
) error {
	createdOn := time.Now().UnixNano()
	createdBy := rxpcontext.Identity(ctx)
	fn := func(tx pgx.Tx) error {
		qs := "INSERT INTO namespaces (domain, name, last_modified_on, last_modified_by) VALUES ($1, $2, $3, $4)"
		_, err := tx.Exec(ctx, qs, domainEntry.RowID, namespace.Name(), createdOn, createdBy)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == pgerrcode.UniqueViolation {
					return errors.DuplicateName("namespace", namespace.Name())
				}
			}
		}
		return err
	}
	if err := s.dbExec(ctx, fn); err != nil {
		return errors.Internal(
			"failed inserting namespaces record",
			errors.WithWrap(err),
		)
	}
	return nil
}
