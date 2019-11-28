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

-- DROP SCHEMA pypgq CASCADE;
