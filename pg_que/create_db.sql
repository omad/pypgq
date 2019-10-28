
CREATE SCHEMA pypgq;
ALTER TABLE pypgq.job ADD singletonOn timestamp without time zone;
ALTER TABLE pypgq.job ADD CONSTRAINT job_singleton UNIQUE(name, singletonOn);
-- one time truncate because previous schema was inserting each version
TRUNCATE TABLE pypgq.version;
INSERT INTO pypgq.version(version) values('0.0.1');


CREATE TYPE pypgq.job_state AS ENUM ('created','retry','active','complete','expired','cancelled');
ALTER TABLE pypgq.job ALTER COLUMN state SET DATA TYPE pypgq.job_state USING state::pypgq.job_state;
ALTER TABLE pypgq.job DROP CONSTRAINT job_singleton;
ALTER TABLE pypgq.job ADD singletonKey text;
CREATE UNIQUE INDEX job_singletonKey ON pypgq.job (name, singletonKey) WHERE state < 'complete' AND singletonOn IS NULL;
CREATE UNIQUE INDEX job_singletonOn ON pypgq.job (name, singletonOn) WHERE state < 'expired' AND singletonKey IS NULL;
CREATE UNIQUE INDEX job_singletonKeyOn ON pypgq.job (name, singletonOn, singletonKey) WHERE state < 'expired';
-- migrate data to use retry state
UPDATE pypgq.job SET state = 'retry' WHERE state = 'expired' AND retryCount < retryLimit;
-- expired jobs weren't being archived in prev schema
UPDATE pypgq.job SET completedOn = now() WHERE state = 'expired' and retryLimit = retryCount;
-- just using good ole fashioned completedOn
ALTER TABLE pypgq.job DROP COLUMN expiredOn;



ALTER TYPE pypgq.job_state ADD VALUE IF NOT EXISTS 'failed' AFTER 'cancelled';



ALTER TABLE pypgq.job ADD COLUMN priority integer not null default(0);
ALTER TABLE pypgq.job ALTER COLUMN createdOn SET DATA TYPE timestamptz;
ALTER TABLE pypgq.job ALTER COLUMN startedOn SET DATA TYPE timestamptz;
ALTER TABLE pypgq.job ALTER COLUMN completedOn SET DATA TYPE timestamptz;



ALTER TABLE pypgq.job ALTER COLUMN startIn SET DEFAULT (interval '0');
ALTER TABLE pypgq.job ALTER COLUMN state SET DEFAULT ('created');
UPDATE pypgq.job SET name = left(name, -9) || '__state__expired' WHERE name LIKE '%__expired';



CREATE INDEX job_fetch ON pypgq.job (priority desc, createdOn, id) WHERE state < 'active';


CREATE TABLE IF NOT EXISTS pypgq.archive (LIKE pypgq.job);
ALTER TABLE pypgq.archive ADD archivedOn timestamptz NOT NULL DEFAULT now();


CREATE EXTENSION IF NOT EXISTS pgcrypto;
ALTER TABLE pypgq.job ALTER COLUMN id SET DEFAULT gen_random_uuid();
ALTER TABLE pypgq.job ADD retryDelay integer not null DEFAULT (0);
ALTER TABLE pypgq.job ADD retryBackoff boolean not null DEFAULT false;
ALTER TABLE pypgq.job ADD startAfter timestamp with time zone not null default now();
UPDATE pypgq.job SET startAfter = createdOn + startIn;
ALTER TABLE pypgq.job DROP COLUMN startIn;
UPDATE pypgq.job SET expireIn = interval '15 minutes' WHERE expireIn IS NULL;
ALTER TABLE pypgq.job ALTER COLUMN expireIn SET NOT NULL;
ALTER TABLE pypgq.job ALTER COLUMN expireIn SET DEFAULT interval '15 minutes';
-- archive table schema changes
ALTER TABLE pypgq.archive ADD retryDelay integer not null DEFAULT (0);
ALTER TABLE pypgq.archive ADD retryBackoff boolean not null DEFAULT false;
ALTER TABLE pypgq.archive ADD startAfter timestamp with time zone;
UPDATE pypgq.archive SET startAfter = createdOn + startIn;
ALTER TABLE pypgq.archive DROP COLUMN startIn;
-- rename complete to completed for state enum - can't use ALTER TYPE :(
DROP INDEX pypgq.job_fetch;
DROP INDEX pypgq.job_singletonOn;
DROP INDEX pypgq.job_singletonKeyOn;
DROP INDEX pypgq.job_singletonKey;
ALTER TABLE pypgq.job ALTER COLUMN state DROP DEFAULT;
ALTER TABLE pypgq.job ALTER COLUMN state SET DATA TYPE text USING state::text;
ALTER TABLE pypgq.archive ALTER COLUMN state SET DATA TYPE text USING state::text;
DROP TYPE pypgq.job_state;
CREATE TYPE pypgq.job_state AS ENUM ('created', 'retry', 'active', 'completed', 'expired', 'cancelled', 'failed');
UPDATE pypgq.job SET state = 'completed' WHERE state = 'complete';
UPDATE pypgq.archive SET state = 'completed' WHERE state = 'complete';
ALTER TABLE pypgq.job ALTER COLUMN state SET DATA TYPE pypgq.job_state USING state::pypgq.job_state;
ALTER TABLE pypgq.job ALTER COLUMN state SET DEFAULT 'created';
ALTER TABLE pypgq.archive ALTER COLUMN state SET DATA TYPE pypgq.job_state USING state::pypgq.job_state;
CREATE INDEX job_fetch ON pypgq.job (name, priority desc, createdOn, id) WHERE state < 'active';
CREATE UNIQUE INDEX job_singletonOn ON pypgq.job (name, singletonOn) WHERE state < 'expired' AND singletonKey IS NULL;
CREATE UNIQUE INDEX job_singletonKeyOn ON pypgq.job (name, singletonOn, singletonKey) WHERE state < 'expired';
CREATE UNIQUE INDEX job_singletonKey ON pypgq.job (name, singletonKey) WHERE state < 'completed' AND singletonOn IS NULL;
-- add new job name index
CREATE INDEX job_name ON pypgq.job (name) WHERE state < 'active';



DROP INDEX pypgq.job_fetch;
DROP INDEX pypgq.job_name;
CREATE INDEX job_name ON pypgq.job (name text_pattern_ops);
UPDATE pypgq.job set name = '__state__completed__' || substr(name, 1, position('__state__completed' in name) - 1) WHERE name LIKE '%__state__completed';



CREATE INDEX archive_id_idx ON pypgq.archive(id);