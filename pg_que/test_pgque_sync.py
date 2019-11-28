
import pytest
from pg_que import PGQue
from hypothesis import given
from hypothesis import strategies as st

CONN_STRING = 'postgres://postgres@localhost/queue_tester'

def test_basics():
    pgque = PGQue('postgres://user:pass@host/database')
    q = pgque.get_queue('testq')
    q.send_message('hello')

    pgque = PGQue('postgres://user:pass@host/database')
    q = pgque.get_queue('testq')
    message = q.receive_message()
    assert message == 'hello'


@given(st.text(), st.lists(st.text(), min_size=1))
def test_pgque(queue_name, messages):
    pgque = PGQue(CONN_STRING)
    q = pgque.get_queue(queue_name)
    for msg in messages:
        q.send_message(msg)

    received = []
    while True:
        msg = q.receive_message()
        if msg is None:
            break
        received.append(msg)

    assert messages == received

