
import pytest
from pg_que import PGQue


def test_basics():
    pgque = PGQue('postgres://user:pass@host/database')
    q = pgque.get_queue('testq')
    q.send_message('hello')

    pgque = PGQue('postgres://user:pass@host/database')
    q = pgque.get_queue('testq')
    message = q.receive_message()
    assert message == 'hello'
