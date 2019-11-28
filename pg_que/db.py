import json

import psycopg2
import uuid
from datetime import timedelta


from pg_que import plans

SCHEMA = 'pypgq'
DEFAULT_OPTS = {}


class Boss:
    def __init__(self, connstring):
        self.connstring = connstring
        self.connection = None

    def start(self):
        self.connection = psycopg2.connect(self.connstring)
        self.init_db()
        # TODO Set up some of the housekeeping tasks

    def prune_queue(self):
        with self.connection.acquire() as conn:
            pass

    def stop(self):
        self.connection.close()

    def get_queue(self, name):
        return Queue(self.connection, name)

    def init_db(self):
        pass


class Queue:
    def __init__(self, pool, name):
        self.next_job_command = plans.fetch_next_message(SCHEMA)
        self.insert_job_command = plans.insertJob(SCHEMA)
        self.pool = pool
        self.name = name

    def receive_messages(self, batch_size=1):
        with self.pool.acquire() as conn:
            jobs = conn.fetch(self.next_job_command, self.name, batch_size)
        return jobs

    def send_message(self, data, options=None):
        if options is None:
            options = DEFAULT_OPTS.copy()

        retry_limit = 1
        retry_delay = 1
        retry_backoff = False
        priority = 0
        id = uuid.uuid4()
        start_after = None
        expireIn = timedelta(days=7)
        singletonKey = None
        singletonSeconds = None
        singletonOffset = None

        #  ords! [1,  2,         3,        4,           5,           6,        7,    8,            9,
        values = [id, self.name, priority, retry_limit, start_after, expireIn, data, singletonKey, singletonSeconds,
                  # 10,            11,         12          ]
                  singletonOffset, retry_delay, retry_backoff]

        with self.pool.acquire() as conn:
            return conn.fetchval(self.insert_job_command, *values)


class Message:
    def complete(self, id):
        pass

    def fail(self, id):
        pass

    def cancel(self, id):
        pass
