import asyncio
import time

import aiosql
import click
import psycopg2

from pg_que.db import Boss

DEFAULT_SCHEMA = 'pypgq'
CONN_STRING = 'postgres://postgres@localhost/queue_tester'


@click.command()
def main():
    queries = aiosql.from_path('creation.sql', 'psycopg2')

    conn = psycopg2.connect(CONN_STRING)

    print(conn)
    # queries.create_schema(conn)
    # conn.commit()

    conn.close()

    asyncio.run(run_perf_tests())


async def run_perf_tests(n=10000):
    boss = Boss(CONN_STRING)
    await boss.start()

    queue = boss.get_queue('test_queue')

    start = time.perf_counter()
    for n in range(n):
        await queue.send_message({'num': n})
    end = time.perf_counter()
    took = end - start

    print(f'Took {took} to send {n + 1} messages. {took / (n + 1)} per message.')

    start = time.perf_counter()
    more_messages = True
    while more_messages:
        m = await queue.receive_messages(batch_size=10)
        if not m:
            more_messages = False
    end = time.perf_counter()
    took = end - start

    print(f'Took {took} to receive {n + 1} messages. {took / (n + 1)} per message.')


if __name__ == '__main__':
    main()
