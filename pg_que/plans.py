# todo change to enum
class states:
    created = 'created'
    retry = 'retry'
    active = 'active'
    completed = 'completed'
    expired = 'expired'
    cancelled = 'cancelled'
    failed = 'failed'


completedmessagePrefix = f'__state__${states.completed}__'


def delete_queue(schema):
    return f'''DELETE FROM {schema}.message WHERE name = $1'''


def delete_all_queues(schema):
    return f'''TRUNCATE {schema}.message'''


def get_version(schema):
    return f'''
    SELECT version from {schema}.version
  '''


def version_table_exists(schema):
    return f'''
    SELECT to_regclass('{schema}.version') as name
  '''


def insert_version(schema):
    return f'''
    INSERT INTO {schema}.version(version) VALUES ($1)
  '''


def fetch_next_message(schema):
    return f'''
    WITH nextmessage as (
      SELECT id
      FROM {schema}.message
      WHERE state < '{states.active}'
        AND name LIKE $1
        AND startAfter < now()
      ORDER BY priority desc, createdOn, id
      LIMIT $2
      FOR UPDATE SKIP LOCKED
    )
    UPDATE {schema}.message j SET
      state = '{states.active}',
      startedOn = now(),
      retryCount = CASE WHEN state = '{states.retry}' THEN retryCount + 1 ELSE retryCount END
    FROM nextmessage
    WHERE j.id = nextmessage.id
    RETURNING j.id, name, data
  '''


def build_json_completion_object(with_response):
    # message completion contract
    return f'''jsonb_build_object(
    'request', jsonb_build_object('id', id, 'name', name, 'data', data),
    'response', {'$2::jsonb' if with_response else 'null'},
    'state', state,
    'retryCount', retryCount,
    'createdOn', createdOn,
    'startedOn', startedOn,
    'completedOn', completedOn,
    'failed', CASE WHEN state = '{states.completed}' THEN false ELSE true END
  )'''


def complete_message(schema):
    return f'''
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
    RETURNING 1
  '''  # returning 1 here just to count results against input array


def fail_messages(schema):
    return f'''
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
  '''  # returning 1 here just to count results against input array


def expire(schema):
    return f'''
    WITH results AS (
      UPDATE {schema}.message
      SET state = CASE
          WHEN retryCount < retryLimit THEN '{states.retry}'::{schema}.message_state
          ELSE '{states.expired}'::{schema}.message_state
          END,        
        completedOn = {RETRY_COMPLETED_ON_CASE},
        startAfter = {RETRY_START_AFTER_CASE}
      WHERE state = '{states.active}'
        AND (startedOn + expireIn) < now()    
      RETURNING *
    )
    INSERT INTO {schema}.message (name, data)
    SELECT
      '{completedmessagePrefix}' || name,
      {build_json_completion_object()}
    FROM results
    WHERE state = '{states.expired}'
      AND NOT name LIKE '{completedmessagePrefix}%'
  '''


def cancel_messages(schema):
    return f'''
    UPDATE {schema}.message
    SET completedOn = now(),
      state = '{states.cancelled}'
    WHERE id IN (SELECT UNNEST($1::uuid[]))
      AND state < '{states.completed}'
    RETURNING 1
  '''  # returning 1 here just to count results against input array


def insert_message(schema):
    return f'''
    INSERT INTO {schema}.message (
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
      DELETE FROM {schema}.message
      WHERE
        (completedOn + CAST($1 as interval) < now())
        OR (
          state = '{states.created}'
          AND name LIKE '{completedmessagePrefix}%'
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


def count_states(schema):
    return f'''
    SELECT name, state, count(*) size
    FROM {schema}.message
    WHERE name NOT LIKE '{completedmessagePrefix}%'
    GROUP BY rollup(name), rollup(state)
  '''


RETRY_COMPLETED_ON_CASE = f'''CASE
          WHEN retryCount < retryLimit
          THEN NULL
          ELSE now()
          END'''

RETRY_START_AFTER_CASE = f'''CASE
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
