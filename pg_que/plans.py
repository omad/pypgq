class states:
    created = 'created'
    retry = 'retry'
    active = 'active'
    completed = 'completed'
    expired = 'expired'
    cancelled = 'cancelled'
    failed = 'failed'


completedJobPrefix = f'__state__${states.completed}__'


def create(schema):
    return [
        createSchema(schema),
        tryCreateCryptoExtension(),
        createVersionTable(schema),
        createJobStateEnum(schema),
        createJobTable(schema),
        cloneJobTableForArchive(schema),
        addIndexToArchive(schema),
        addArchivedOnToArchive(schema),
        createIndexJobName(schema),
        createIndexSingletonOn(schema),
        createIndexSingletonKeyOn(schema),
        createIndexSingletonKey(schema)
    ]


def createSchema(schema):
    return f'CREATE SCHEMA IF NOT EXISTS {schema}'


def tryCreateCryptoExtension():
    return f'CREATE EXTENSION IF NOT EXISTS pgcrypto'


def createVersionTable(schema):
    return f'''
    CREATE TABLE {schema}.version (
      version text primary key
    )
    '''


def createJobStateEnum(schema):
    ## ENUM definition order is important
    ## base type is numeric and first values are less than last values
    return f'''
    CREATE TYPE {schema}.job_state AS ENUM (
      '{states.created}',
      '{states.retry}',
      '{states.active}',	
      '{states.completed}',
      '{states.expired}',
      '{states.cancelled}',
      '{states.failed}'
    )
    '''


def createJobTable(schema):
    return f'''
    CREATE TABLE {schema}.job (
      id uuid primary key not null default gen_random_uuid(),
      name text not null,
      priority integer not null default(0),
      data jsonb,
      state {schema}.job_state not null default('{states.created}'),
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
    )
    '''


def cloneJobTableForArchive(schema):
    return f'''CREATE TABLE {schema}.archive (LIKE {schema}.job)'''


def addArchivedOnToArchive(schema):
    return f'''ALTER TABLE {schema}.archive ADD archivedOn timestamptz NOT NULL DEFAULT now()'''


def addIndexToArchive(schema):
    return f'''CREATE INDEX archive_id_idx ON {schema}.archive(id)f'''


def deleteQueue(schema):
    return f'''DELETE FROM {schema}.job WHERE name = $1'''


def deleteAllQueues(schema):
    return f'''TRUNCATE {schema}.job'''


def createIndexSingletonKey(schema):
    # anything with singletonKey means "only 1 job can be queued or active at a time"
    return f'''
    CREATE UNIQUE INDEX job_singletonKey ON {schema}.job (name, singletonKey) WHERE state < '{states.completed}' AND singletonOn IS NULL
  '''


def createIndexSingletonOn(schema):
    # anything with singletonOn means "only 1 job within this time period, queued, active or completed"
    return f'''
    CREATE UNIQUE INDEX job_singletonOn ON {schema}.job (name, singletonOn) WHERE state < '{states.expired}' AND singletonKey IS NULL
  '''


def createIndexSingletonKeyOn(schema):
    # anything with both singletonOn and singletonKey means "only 1 job within this time period with this key, queued, active or completed"
    return f'''
    CREATE UNIQUE INDEX job_singletonKeyOn ON {schema}.job (name, singletonOn, singletonKey) WHERE state < '{states.expired}'
  '''


def createIndexJobName(schema):
    return f'''
    CREATE INDEX job_name ON {schema}.job (name text_pattern_ops)
  '''


def getVersion(schema):
    return f'''
    SELECT version from {schema}.version
  '''


def versionTableExists(schema):
    return f'''
    SELECT to_regclass('{schema}.version') as name
  '''


def insertVersion(schema):
    return f'''
    INSERT INTO {schema}.version(version) VALUES ($1)
  '''


def fetchNextJob(schema):
    return f'''
    WITH nextJob as (
      SELECT id
      FROM {schema}.job
      WHERE state < '{states.active}'
        AND name LIKE $1
        AND startAfter < now()
      ORDER BY priority desc, createdOn, id
      LIMIT $2
      FOR UPDATE SKIP LOCKED
    )
    UPDATE {schema}.job j SET
      state = '{states.active}',
      startedOn = now(),
      retryCount = CASE WHEN state = '{states.retry}' THEN retryCount + 1 ELSE retryCount END
    FROM nextJob
    WHERE j.id = nextJob.id
    RETURNING j.id, name, data
  '''


def buildJsonCompletionObject(withResponse):
    # job completion contract
    return f'''jsonb_build_object(
    'request', jsonb_build_object('id', id, 'name', name, 'data', data),
    'response', {'$2::jsonb' if withResponse else 'null'},
    'state', state,
    'retryCount', retryCount,
    'createdOn', createdOn,
    'startedOn', startedOn,
    'completedOn', completedOn,
    'failed', CASE WHEN state = '{states.completed}' THEN false ELSE true END
  )'''


retryCompletedOnCase = f'''CASE
          WHEN retryCount < retryLimit
          THEN NULL
          ELSE now()
          END'''

retryStartAfterCase = f'''CASE
          WHEN retryCount = retryLimit THEN startAfter
          WHEN NOT retryBackoff THEN now() + retryDelay * interval '1'
          ELSE now() +
            (
                retryDelay * 2 ^ LEAST(16, retryCount + 1) / 2
                +
                retryDelay * 2 ^ LEAST(16, retryCount + 1) / 2 * random()
            )
            * interval '1'
          END'''


def completeJobs(schema):
    return f'''
    WITH results AS (
      UPDATE {schema}.job
      SET completedOn = now(),
        state = '{states.completed}'
      WHERE id IN (SELECT UNNEST($1::uuid[]))
        AND state = '{states.active}'
      RETURNING *
    )
    INSERT INTO {schema}.job (name, data)
    SELECT
      '{completedJobPrefix}' || name, 
      {buildJsonCompletionObject(True)}
    FROM results
    WHERE NOT name LIKE '{completedJobPrefix}%'
    RETURNING 1
  '''  # returning 1 here just to count results against input array


def failJobs(schema):
    return f'''
    WITH results AS (
      UPDATE {schema}.job
      SET state = CASE
          WHEN retryCount < retryLimit
          THEN '{states.retry}'::{schema}.job_state
          ELSE '{states.failed}'::{schema}.job_state
          END,        
        completedOn = {retryCompletedOnCase},
        startAfter = {retryStartAfterCase}
      WHERE id IN (SELECT UNNEST($1::uuid[]))
        AND state < '{states.completed}'
      RETURNING *
    )
    INSERT INTO {schema}.job (name, data)
    SELECT
      '{completedJobPrefix}' || name,
      {buildJsonCompletionObject(True)}
    FROM results
    WHERE state = '{states.failed}'
      AND NOT name LIKE '{completedJobPrefix}%'
    RETURNING 1
  '''  # returning 1 here just to count results against input array


def expire(schema):
    return f'''
    WITH results AS (
      UPDATE {schema}.job
      SET state = CASE
          WHEN retryCount < retryLimit THEN '{states.retry}'::{schema}.job_state
          ELSE '{states.expired}'::{schema}.job_state
          END,        
        completedOn = {retryCompletedOnCase},
        startAfter = {retryStartAfterCase}
      WHERE state = '{states.active}'
        AND (startedOn + expireIn) < now()    
      RETURNING *
    )
    INSERT INTO {schema}.job (name, data)
    SELECT
      '{completedJobPrefix}' || name,
      {buildJsonCompletionObject()}
    FROM results
    WHERE state = '{states.expired}'
      AND NOT name LIKE '{completedJobPrefix}%'
  '''


def cancelJobs(schema):
    return f'''
    UPDATE {schema}.job
    SET completedOn = now(),
      state = '{states.cancelled}'
    WHERE id IN (SELECT UNNEST($1::uuid[]))
      AND state < '{states.completed}'
    RETURNING 1
  '''  # returning 1 here just to count results against input array


def insertJob(schema):
    return f'''
    INSERT INTO {schema}.job (
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
      $1,
      $2,
      $3,
      '{states.created}',
      $4, 
      CASE WHEN right($5, 1) = 'Z' THEN CAST($5 as timestamp with time zone) ELSE now() + CAST(COALESCE($5,'0') as interval) END,
      CAST($6 as interval),
      $7,
      $8,
      CASE WHEN $9::integer IS NOT NULL THEN 'epoch'::timestamp + '1 second'::interval * ($9 * floor((date_part('epoch', now()) + $10) / $9)) ELSE NULL END,
      $11,
      $12
    )
    ON CONFLICT DO NOTHING
    RETURNING id
  '''


def purge(schema):
    return f'''
    DELETE FROM {schema}.archive
    WHERE (archivedOn + CAST($1 as interval) < now())
  '''


def archive(schema):
    return f'''
    WITH archived_rows AS (
      DELETE FROM {schema}.job
      WHERE
        (completedOn + CAST($1 as interval) < now())
        OR (
          state = '{states.created}'
          AND name LIKE '{completedJobPrefix}%'
          AND createdOn + CAST($1 as interval) < now()
        )
      RETURNING *
    )
    INSERT INTO {schema}.archive (
      id, name, priority, data, state, retryLimit, retryCount, retryDelay, retryBackoff, startAfter, startedOn, singletonKey, singletonOn, expireIn, createdOn, completedOn
    )
    SELECT 
      id, name, priority, data, state, retryLimit, retryCount, retryDelay, retryBackoff, startAfter, startedOn, singletonKey, singletonOn, expireIn, createdOn, completedOn
    FROM archived_rows
  '''


def countStates(schema):
    return f'''
    SELECT name, state, count(*) size
    FROM {schema}.job
    WHERE name NOT LIKE '{completedJobPrefix}%'
    GROUP BY rollup(name), rollup(state)
  '''
