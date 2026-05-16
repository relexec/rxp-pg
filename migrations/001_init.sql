-- +goose up
CREATE TABLE namescopes (
  id SMALLINT NOT NULL PRIMARY KEY
, name TEXT NOT NULL
, description TEXT NOT NULL
);

INSERT INTO namescopes (id, name, description)
VALUES
  (1, 'namespace', 'The name is unique within the scope of the object system, kind, domain and namespace.')
, (2, 'domain', 'The name is unique within the scope of the object system, kind and domain.')
, (3, 'system', 'The name is unique within the scope of the object system and kind.')
;

CREATE TABLE systems (
  id SERIAL NOT NULL PRIMARY KEY
, uuid UUID NOT NULL
, name TEXT NULL
, host BOOLEAN DEFAULT FALSE
, UNIQUE (uuid)
);

CREATE TABLE kinds (
  id SERIAL NOT NULL PRIMARY KEY
, system INT NOT NULL
, name TEXT NOT NULL
, namescope SMALLINT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, UNIQUE (system, name)
);

CREATE TABLE domains (
  id SERIAL NOT NULL PRIMARY KEY
, system INT NOT NULL
, uuid UUID NOT NULL
, name TEXT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, UNIQUE (uuid)
, UNIQUE (system, name)
);

CREATE TABLE domains_archived (
  domain INT NOT NULL PRIMARY KEY
, system INT NOT NULL
, uuid UUID NOT NULL
, name TEXT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, archived_on BIGINT NULL
, archived_by TEXT NULL
);

CREATE TABLE namespaces (
  id SERIAL NOT NULL PRIMARY KEY
, domain INT NOT NULL
, name TEXT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, UNIQUE (domain, name)
);

CREATE TABLE namespaces_archived (
  namespace INT NOT NULL PRIMARY KEY
, domain INT NOT NULL
, name TEXT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, archived_on BIGINT NULL
, archived_by TEXT NULL
);

CREATE TABLE metas (
  id SERIAL NOT NULL PRIMARY KEY
, system INT NOT NULL
, kind INT NOT NULL
, version TEXT NOT NULL
, schema TEXT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, UNIQUE (system, kind, version)
);

CREATE TABLE metas_archived (
  meta INT NOT NULL PRIMARY KEY
, system INT NOT NULL
, kind INT NOT NULL
, version TEXT NOT NULL
, schema TEXT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, archived_on BIGINT NULL
, archived_by TEXT NULL
);

CREATE TABLE objects (
  id BIGSERIAL NOT NULL PRIMARY KEY
, system INT NOT NULL
, meta INT NOT NULL
, uuid UUID NOT NULL
, generation INT NOT NULL
, domain INT NULL
, namespace INT NULL
, spec TEXT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, UNIQUE (uuid)
);

CREATE INDEX ix_objects_divisions
ON objects (system, meta, domain, namespace);

CREATE TABLE system_qualified_object_names (
  object BIGINT NOT NULL PRIMARY KEY
, system INT NOT NULL
, kind INT NOT NULL
, name TEXT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, UNIQUE (system, kind, name)
);

CREATE TABLE domain_qualified_object_names (
  object BIGINT NOT NULL PRIMARY KEY
, system INT NOT NULL
, kind INT NOT NULL
, domain INT NOT NULL
, name TEXT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, UNIQUE (system, kind, domain, name)
);

CREATE TABLE namespace_qualified_object_names (
  object BIGINT NOT NULL PRIMARY KEY
, system INT NOT NULL
, kind INT NOT NULL
, namespace INT NOT NULL
, name TEXT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, UNIQUE (system, kind, namespace, name)
);

CREATE TABLE objects_archived (
  object BIGINT NOT NULL PRIMARY KEY
, system INT NOT NULL
, meta INT NOT NULL
, uuid UUID NOT NULL
, domain INT NULL
, namespace INT NULL
, name TEXT NOT NULL
, spec TEXT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, archived_on BIGINT NOT NULL
, archived_by TEXT NOT NULL
, UNIQUE (uuid)
);

CREATE TABLE object_generations (
  object BIGINT NOT NULL
, generation INT NOT NULL
, meta INT NOT NULL
, created_on BIGINT NOT NULL
, created_by TEXT NOT NULL
, spec TEXT NOT NULL
, PRIMARY KEY (object, generation)
);

CREATE INDEX ix_object_generations_meta
ON object_generations (meta);

CREATE TABLE object_generations_archived (
  object BIGINT NOT NULL
, generation INT NOT NULL
, meta INT NOT NULL
, spec TEXT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, archived_on BIGINT NULL
, archived_by TEXT NULL
, PRIMARY KEY (object, generation)
);

CREATE TABLE object_labels (
  object BIGINT NOT NULL
, key TEXT NOT NULL
, value TEXT NOT NULL
, PRIMARY KEY (object, key, value)
);

-- +goose down
DROP TABLE object_labels;
DROP TABLE system_qualified_object_names;
DROP TABLE domain_qualified_object_names;
DROP TABLE namespace_qualified_object_names;
DROP TABLE object_generations_archived;
DROP TABLE object_generations;
DROP TABLE objects_archived;
DROP TABLE objects;
DROP TABLE metas_archived;
DROP TABLE metas;
DROP TABLE namespaces_archived;
DROP TABLE namespaces;
DROP TABLE domains_archived;
DROP TABLE domains;
DROP TABLE kinds;
DROP TABLE systems;
DROP TABLE namescopes;
