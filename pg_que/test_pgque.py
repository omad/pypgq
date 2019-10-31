import pytest

from pg_que.db import Boss


@pytest.mark.asyncio
async def test_queue():
    boss = Boss('')
    await boss.start()

    queue = boss.get_queue('test_queue')

    for n in range(10):
        await queue.send_message({'num': n})

    for n in range(11):
        m = await queue.receive_messages()
        print(m)
