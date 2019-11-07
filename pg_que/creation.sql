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

CREATE TYPE pypgq.job_state AS ENUM ('created','retry','active','complete','expired','cancelled', 'failed');

CREATE TABLE pypgq.job (
  id uuid primary key not null default gen_random_uuid(),
  name text not null,
  priority integer not null default(0),
  data jsonb,
  state pypgq.job_state not null default('created'),
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

-- clone job table for archived
CREATE TABLE pypgq.archive (LIKE pypgq.job);
ALTER TABLE pypgq.archive ADD archivedOn timestamptz NOT NULL DEFAULT now();
CREATE INDEX archive_id_idx ON pypgq.archive(id);

CREATE INDEX job_name ON pypgq.job (name text_pattern_ops);


-- one time truncate because previous schema was inserting each version
TRUNCATE TABLE pypgq.version;
INSERT INTO pypgq.version(version) values('0.0.1');


ALTER TABLE pypgq.job ALTER COLUMN state SET DATA TYPE pypgq.job_state USING state::pypgq.job_state;
CREATE UNIQUE INDEX job_singletonKey ON pypgq.job (name, singletonKey) WHERE state < 'complete' AND singletonOn IS NULL;
CREATE UNIQUE INDEX job_singletonOn ON pypgq.job (name, singletonOn) WHERE state < 'expired' AND singletonKey IS NULL;
CREATE UNIQUE INDEX job_singletonKeyOn ON pypgq.job (name, singletonOn, singletonKey) WHERE state < 'expired';


CREATE INDEX job_fetch ON pypgq.job (priority desc, createdOn, id) WHERE state < 'active';



-- name: delete-everything#
-- Drop the entire schema
DROP SCHEMA pypgq CASCADE;
