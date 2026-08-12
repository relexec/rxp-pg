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

CREATE TABLE run_event_types (
  id SMALLINT NOT NULL PRIMARY KEY
, name TEXT NOT NULL
, description TEXT NOT NULL
);

INSERT INTO run_event_types (id, name, description)
VALUES
  (0, 'run.scheduled', 'When the Run was scheduled. Scheduled may be different from Started if the caller has requested execution of a Runnable at a future time.')
, (1, 'run.started', 'When the Runnable started to execute.')
, (2, 'run.completed', 'When the Runnable completed execution successfully (no application-layer terminal failures were encountered).')
, (3, 'run.failed', 'When the Runnable completed execution and encountered an application-layer terminal failure.')
, (4, 'run.canceled', 'When a manual call to cancel an ongoing Run was made.')
, (5, 'run.timedout', 'When the execution of a Runnable hit a configured timeout duration.')
, (6, 'run.paused', 'When a manual call to pause an ongoing Run was made.')
, (7, 'run.resumed', 'When a manual call to resume a paused Run was made.')
, (8, 'timer.started', 'When a timer is started within the execution of a Runnable.')
, (9, 'timer.fired', 'When a timer fires (wakes).')
;

CREATE TABLE run_events (
  id BIGSERIAL NOT NULL PRIMARY KEY
, run BIGINT NOT NULL
, sequence INT NOT NULL
, event_type INT NOT NULL
, occurred_on BIGINT NOT NULL
, UNIQUE (run, sequence)
);

CREATE TABLE run_events_archived (
  event BIGINT NOT NULL
, run BIGINT NOT NULL
, sequence INT NOT NULL
, event_type INT NOT NULL
, occurred_on BIGINT NOT NULL
, archived_on BIGINT NOT NULL
, archived_by TEXT NOT NULL
);

-- +goose down
DROP TABLE run_events_archived;
DROP TABLE run_events;
DROP TABLE run_requests_archived;
DROP TABLE run_requests;
DROP TABLE runs_archived;
DROP TABLE runs;
