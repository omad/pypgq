--
-- The SQL in this file is used to create and destroy database objects for PyPGQ
--
-- It is loaded and run by the `aiosql` library, which splits based on `name:` tags
--

-- name: create-schema#
-- create the schema, types and tables
CREATE SCHEMA IF NOT EXISTS pypgq;


CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE  IF NOT EXISTS pypgq.version ( version text primary key );

CREATE TYPE pypgq.message_state AS ENUM ('created','retry','active','completed','expired','cancelled', 'failed');

CREATE TABLE pypgq.message (
  id uuid primary key not null default gen_random_uuid(),
  name text not null,
  priority integer not null default(0),
  data jsonb,
  state pypgq.message_state not null default('created'),
  retryLimit integer not null default(0),
  retryCount integer not null default(0),
  retryDelay integer not null default(0),
  retryBackoff boolean not null default false,
  startAfter timestamp with time zone not null default now(),
  startedOn timestamp with time zone,
  singletonKey text,
  singletonOn timestamp without time zone,
  expireIn interval not null default interval '15 minutes',
  createdOn timestamp with time zone not null default now(),
  completedOn timestamp with time zone
);


-- clone message table for archived
CREATE TABLE pypgq.archive (LIKE pypgq.message);
ALTER TABLE pypgq.archive ADD archivedOn timestamptz NOT NULL DEFAULT now();
CREATE INDEX archive_id_idx ON pypgq.archive(id);

CREATE INDEX message_name ON pypgq.message (name text_pattern_ops);


-- one time truncate because previous schema was inserting each version
TRUNCATE TABLE pypgq.version;
INSERT INTO pypgq.version(version) values('0.0.1');


ALTER TABLE pypgq.message ALTER COLUMN state SET DATA TYPE pypgq.message_state USING state::pypgq.message_state;
-- # anything with singletonKey means "only 1 message can be queued or active at a time"
CREATE UNIQUE INDEX message_singletonKey ON pypgq.message (name, singletonKey) WHERE state < 'complete' AND singletonOn IS NULL;
-- # anything with singletonOn means "only 1 message within this time period, queued, active or completed"
CREATE UNIQUE INDEX message_singletonOn ON pypgq.message (name, singletonOn) WHERE state < 'expired' AND singletonKey IS NULL;

-- anything with both singletonOn and singletonKey means "only 1 message within this time period with this key, queued, active or completed"
CREATE UNIQUE INDEX message_singletonKeyOn ON pypgq.message (name, singletonOn, singletonKey) WHERE state < 'expired';


CREATE INDEX message_fetch ON pypgq.message (priority desc, createdOn, id) WHERE state < 'active';



-- name: delete-everything#
-- Drop the entire schema

DROP SCHEMA pypgq CASCADE;


-- name: expire
--

WITH results AS (
    UPDATE pypgq.message
        SET state = CASE
        WHEN retryCount < retryLimit THEN 'retry'::pypgq.message_state
        ELSE 'expired'::pypgq.message_state
        END,
        completedOn = CASE
          WHEN retryCount < retryLimit
          THEN NULL
          ELSE now()
          END,
        startAfter = CASE
          WHEN retryCount = retryLimit THEN startAfter
          WHEN NOT retryBackoff THEN now() + retryDelay * interval '1'
          ELSE now() +
            (
                retryDelay * 2 ^ LEAST(16, retryCount + 1) / 2
                +
                retryDelay * 2 ^ LEAST(16, retryCount + 1) / 2 * random()
            )
            * interval '1'
          END
        WHERE state = 'active'
        AND (startedOn + expireIn) < now()
        RETURNING *
)
INSERT INTO pypgq.message (name, data)
SELECT
        '__state__$completed__' || name,
    {build_json_completion_object()}
FROM results
WHERE state = 'expired'
  AND NOT name LIKE '__state__$completed__';


-- name: cancel_messages

UPDATE pypgq.message
SET completedOn = now(),
      state = 'cancelled'
    WHERE id IN (SELECT UNNEST(:ids::uuid[]))
      AND state < 'completed'
    RETURNING 1;
-- # returning 1 here just to count results against input array


-- name: insert_message

INSERT INTO pypgq.message (
    id,
    name,
    priority,
    state,
    retryLimit,
    startAfter,
    expireIn,
    data,
    singletonKey,
    singletonOn,
    retryDelay,
    retryBackoff
)
VALUES (
           :id,
           :name,
           :priority,
           'created',
           :retry_limit,
           CASE WHEN right(:start_after, 1) = 'Z' THEN CAST(:start_after as timestamp with time zone) ELSE now() + CAST(COALESCE(:start_after,'0') as interval) END,
           CAST(:expire_in as interval),
           :data,
           :singleton_key,
           CASE WHEN :singleton_seconds::integer IS NOT NULL THEN 'epoch'::timestamp + '1 second'::interval * (:singleton_seconds * floor((date_part('epoch', now()) + :singleton_offset) / :singleton_seconds)) ELSE NULL END,
           :retry_delay,
           :retry_backoff
       )
    ON CONFLICT DO NOTHING
RETURNING id;


-- name: fetch_next_message
WITH nextmessage as (
    SELECT id
    FROM pypgq.message
    WHERE state < 'active'
      AND name LIKE $1
      AND startAfter < now()
    ORDER BY priority desc, createdOn, id
        LIMIT $2
        FOR UPDATE SKIP LOCKED
)
UPDATE pypgq.message j SET
    state = 'active',
    startedOn = now(),
    retryCount = CASE WHEN state = 'retry' THEN retryCount + 1 ELSE retryCount END
    FROM nextmessage
    WHERE j.id = nextmessage.id
    RETURNING j.id, name, data;

-- name: delete_all_queries
TRUNCATE {schema}.message;
-- name: get_version
SELECT version from {schema}.version;
-- name: version_table_exists
SELECT version from {schema}.version;

-- name: complete_message
WITH results AS (
    UPDATE {schema}.message
        SET completedOn = now(),
        state = '{states.completed}'
        WHERE id IN (SELECT UNNEST($1::uuid[]))
        AND state = '{states.active}'
        RETURNING *
)
INSERT INTO {schema}.message (name, data)
SELECT
        '{completedmessagePrefix}' || name,
    {build_json_completion_object(True)}
FROM results
WHERE NOT name LIKE '{completedmessagePrefix}%'
    RETURNING 1;
-- returning 1 here just to count results against input array


-- name: fail_messages

WITH results AS (
    UPDATE {schema}.message
        SET state = CASE
        WHEN retryCount < retryLimit
        THEN '{states.retry}'::{schema}.message_state
        ELSE '{states.failed}'::{schema}.message_state
        END,
        completedOn = {RETRY_COMPLETED_ON_CASE},
        startAfter = {RETRY_START_AFTER_CASE}
        WHERE id IN (SELECT UNNEST($1::uuid[]))
        AND state < '{states.completed}'
        RETURNING *
)
INSERT INTO {schema}.message (name, data)
SELECT
        '{completedmessagePrefix}' || name,
    {build_json_completion_object(True)}
FROM results
WHERE state = '{states.failed}'
  AND NOT name LIKE '{completedmessagePrefix}%'
    RETURNING 1
-- returning 1 here just to count results against input array
