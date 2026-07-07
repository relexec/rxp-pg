-- +goose up
CREATE TABLE event_types (
  id INT NOT NULL PRIMARY KEY
, name TEXT NOT NULL
, description TEXT NOT NULL
);

INSERT INTO event_types (id, name, description)
VALUES
  (1, 'run.scheduled', 'Occurs when a request to perform some work in the future is safely persisted.')
, (2, 'run.started', 'Occurs when a request to perform some work in the past or at this exact moment in time is safely persisted.')
, (3, 'run.completed', 'Occurs when some work completed with no application-layer errors.')
, (4, 'run.failed', 'Occurs when some work failed to complete due to an application-layer error.')
, (5, 'run.canceled', 'Occurs when a request to cancel some ongoing work is safely persisted.')
, (6, 'run.paused', 'Occurs when a request to pause some ongoing work is safely persisted.')
, (7, 'run.resumed', 'Occurs when a request to resume some paused work is safely persisted.')
;

CREATE TABLE events (
  id BIGSERIAL NOT NULL PRIMARY KEY
, run BIGINT NOT NULL
, sequence INT NOT NULL
, event_type INT NOT NULL
, occurred_on BIGINT NOT NULL
, UNIQUE (run, sequence)
);

CREATE TABLE events_archived (
  event BIGINT NOT NULL PRIMARY KEY
, run BIGINT NOT NULL
, sequence INT NOT NULL
, event_type INT NOT NULL
, occurred_on BIGINT NOT NULL
, archived_on BIGINT NOT NULL
);

-- +goose down
DROP TABLE events_archived;
DROP TABLE events;
DROP TABLE event_types;
