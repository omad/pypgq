import asyncio
import json
import uuid
from datetime import timedelta

import asyncpg

from pg_que import plans

SCHEMA = 'pypgq'
DEFAULT_OPTS = {}


class Boss:
    def __init__(self, connstring):
        self.connstring = connstring
        self.pool = None

    async def run(self):
        values = await self.conn.fetch('''SELECT * FROM mytable''')

    async def start(self):
        self.pool = await asyncpg.create_pool(self.connstring)
        async with self.pool.acquire() as conn:
            for name in ('json', 'jsonb'):
                await conn.set_type_codec(
                    name,
                    encoder=json.dumps,
                    decoder=json.loads,
                    schema='pg_catalog'
                )
        await self.init_db()
        # TODO Set up some of the housekeeping tasks
        loop = asyncio.get_running_loop()
        loop.call_later(600, self.prune_queue)

    async def prune_queue(self):
        loop = asyncio.get_running_loop()
        loop.call_later(600, self.prune_queue)
        async with self.pool.acquire() as conn:
            pass

    async def stop(self):
        await self.conn.close()

    def get_queue(self, name):
        return Queue(self.pool, name)

    async def init_db(self):
        pass


class Queue:
    def __init__(self, pool, name):
        self.next_job_command = plans.fetchNextJob(SCHEMA)
        self.insert_job_command = plans.insertJob(SCHEMA)
        self.pool = pool
        self.name = name

    async def receive_messages(self, batch_size=1):
        async with self.pool.acquire() as conn:
            jobs = await conn.fetch(self.next_job_command, self.name, batch_size)
        return jobs

    async def send_message(self, data, options=None):
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

        async with self.pool.acquire() as conn:
            return await conn.fetchval(self.insert_job_command, *values)


class Message:
    async def complete(self, id):
        pass

    async def fail(self, id):
        pass

    async def cancel(self, id):
        pass
