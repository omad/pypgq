import asyncio
import uuid

import asyncpg

from pg_que import plans

SCHEMA = 'pypgq'
DEFAULT_OPTS = {}

async def run():
    values = await conn.fetch('''SELECT * FROM mytable''')



class Boss:
    def __init__(self, connstring):
        self.connstring = connstring
        self.next_job_command = plans.fetchNextJob(SCHEMA)
        self.insert_job_command = plans.insertJob(SCHEMA)
        self.conn = None
        pass

    async def start(self):
        self.conn = await asyncpg.connect(user='user', password='password',
                                     database='database', host='127.0.0.1')

    async def stop(self):
        await self.conn.close()

    async def fetch(self, name, batch_size=1):
        jobs = await self.conn.fetch(self.next_job_command, name, batch_size )
        return jobs

    async def publish(self, name, data, options=None):
        if options is None:
            options = DEFAULT_OPTS.copy()

        retry_limit = 1
        retry_delay = 1
        retry_backoff = 1
        priority = 0
        id = uuid.uuid4()
        start_after = None
        expireIn = None
        singletonKey = None
        singletonSeconds = None
        singletonOffset = None

        #  ords! [1,  2,    3,        4,           5,           6,        7,    8,            9,                10,              11,         12          ]
        values = [id, name, priority, retry_limit, start_after, expireIn, data, singletonKey, singletonSeconds, singletonOffset, retry_delay, retry_backoff];

        return await self.conn.fetchval(self.insert_job_command, *values)


    async def complete(self, id):
        pass

    async def fail(self, id):
        pass

    async def cancel(self, id):
        pass


loop = asyncio.get_event_loop()
loop.run_until_complete(run())
