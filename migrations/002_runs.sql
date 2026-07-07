-- +goose up
CREATE TABLE runs (
  id BIGSERIAL NOT NULL PRIMARY KEY
, uuid UUID NOT NULL
, target BIGINT NOT NULL
, root BIGINT NOT NULL
, parent BIGINT NULL
, scheduled_on BIGINT NOT NULL
, started_on BIGINT NULL
, completed_on BIGINT NULL
, failed_on BIGINT NULL
, paused_on BIGINT NULL
, resumed_on BIGINT NULL
, canceled_on BIGINT NULL
, UNIQUE (uuid)
);

CREATE INDEX ix_runs_target
ON runs (target);

CREATE INDEX ix_runs_root_target
ON runs (root, target);

CREATE TABLE runs_archived (
  run BIGINT NOT NULL PRIMARY KEY
, uuid UUID NOT NULL
, target BIGINT NOT NULL
, root BIGINT NOT NULL
, parent BIGINT NULL
, scheduled_on BIGINT NOT NULL
, started_on BIGINT NULL
, completed_on BIGINT NULL
, failed_on BIGINT NULL
, paused_on BIGINT NULL
, resumed_on BIGINT NULL
, canceled_on BIGINT NULL
, archived_on BIGINT NOT NULL
, archived_by TEXT NOT NULL
);

CREATE TABLE run_requests (
  run BIGINT NOT NULL PRIMARY KEY
, created_on BIGINT NOT NULL
, caller_identity TEXT NOT NULL
, caller_system INT NOT NULL
, caller_domain INT NULL
, in_vars TEXT NULL
, options TEXT NULL
);

CREATE TABLE run_requests_archived (
  run BIGINT NOT NULL
, created_on BIGINT NOT NULL
, caller_identity TEXT NOT NULL
, caller_system INT NOT NULL
, caller_domain INT NULL
, in_vars TEXT NOT NULL
, options TEXT NOT NULL
, archived_on BIGINT NOT NULL
, archived_by TEXT NOT NULL
);

-- +goose down
DROP TABLE run_requests_archived;
DROP TABLE run_requests;
DROP TABLE runs_archived;
DROP TABLE runs;
