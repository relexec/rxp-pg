-- +goose up
CREATE TABLE scopes (
  id SMALLINT NOT NULL PRIMARY KEY
, name TEXT NOT NULL
, description TEXT NOT NULL
);

INSERT INTO scopes (id, name, description)
VALUES
  (0, 'domain', 'The name is unique within the scope of the kind and domain.')
, (1, 'system', 'The name is unique within the scope of the kind and system.')
, (2, 'global', 'The type of thing can only be identified by UUID.')
;

CREATE TABLE systems (
  id SERIAL NOT NULL PRIMARY KEY
, uuid UUID NOT NULL
, tag TEXT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, UNIQUE (uuid)
);

CREATE TABLE domains (
  id SERIAL NOT NULL PRIMARY KEY
, system INT NOT NULL
, uuid UUID NOT NULL
, name TEXT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, root INT NOT NULL
, parent INT NULL
, left_side INT NOT NULL
, right_side INT NOT NULL
, UNIQUE (uuid)
, UNIQUE (system, name)
, UNIQUE (root, left_side, right_side)
);

CREATE INDEX ix_domains_parent
ON domains (parent);

CREATE TABLE domains_archived (
  domain INT NOT NULL PRIMARY KEY
, system INT NOT NULL
, uuid UUID NOT NULL
, name TEXT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, archived_on BIGINT NULL
, archived_by TEXT NULL
, parent INT NULL
, left_side INT NOT NULL
, right_side INT NOT NULL
);

CREATE TABLE kinds (
  id SERIAL NOT NULL PRIMARY KEY
, system INT NOT NULL
, uuid UUID NOT NULL
, name TEXT NOT NULL
, scope SMALLINT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, UNIQUE (uuid)
, UNIQUE (system, name)
);

CREATE TABLE kinds_archived (
  kind INT NOT NULL PRIMARY KEY
, system INT NOT NULL
, uuid UUID NOT NULL
, name TEXT NOT NULL
, scope SMALLINT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, archived_on BIGINT NULL
, archived_by TEXT NULL
);

CREATE TABLE kindversions (
  id BIGSERIAL NOT NULL PRIMARY KEY
, system INT NOT NULL
, kind INT NOT NULL
, version TEXT NOT NULL
, schema TEXT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, UNIQUE (system, kind, version)
);

CREATE TABLE kindversions_archived (
  kindversion INT NOT NULL PRIMARY KEY
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
, kindversion BIGINT NOT NULL
, uuid UUID NOT NULL
, generation INT NOT NULL
, domain INT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, UNIQUE (uuid)
);

CREATE INDEX ix_objects_divisions
ON objects (system, kindversion, domain);

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

CREATE TABLE objects_archived (
  object BIGINT NOT NULL PRIMARY KEY
, system INT NOT NULL
, kindversion BIGINT NOT NULL
, uuid UUID NOT NULL
, domain INT NULL
, name TEXT NOT NULL
, generation INT NOT NULL
, last_modified_on BIGINT NOT NULL
, last_modified_by TEXT NOT NULL
, archived_on BIGINT NOT NULL
, archived_by TEXT NOT NULL
, UNIQUE (uuid)
);

CREATE TABLE object_generations (
  id BIGSERIAL NOT NULL PRIMARY KEY
, object BIGINT NOT NULL
, generation INT NOT NULL
, kindversion BIGINT NOT NULL
, created_on BIGINT NOT NULL
, created_by TEXT NOT NULL
, spec TEXT NOT NULL
, UNIQUE (object, generation)
);

CREATE INDEX ix_object_generations_kindversion
ON object_generations (kindversion);

CREATE TABLE object_generations_archived (
  object_generation BIGINT NOT NULL
, object BIGINT NOT NULL
, generation INT NOT NULL
, kindversion INT NOT NULL
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
DROP TABLE object_generations_archived;
DROP TABLE object_generations;
DROP TABLE objects_archived;
DROP TABLE system_qualified_object_names;
DROP TABLE domain_qualified_object_names;
DROP TABLE objects;
DROP TABLE kindversions_archived;
DROP TABLE kindversions;
DROP TABLE kinds_archived;
DROP TABLE kinds;
DROP TABLE domains_archived;
DROP TABLE domains;
DROP TABLE systems;
DROP TABLE scopes;
