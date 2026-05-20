import pytest
from broker.queue_ import TaskQueue
from shared.task import Task


def test_enqueue_dequeue():
    q = TaskQueue()
    t = Task("add", [1, 2])
    q.enqueue(t)
    result = q.dequeue()
    assert result.task_id == t.task_id

def test_priority_order():
    q = TaskQueue()
    low    = Task("add", [1, 2], priority=0)
    high   = Task("add", [1, 2], priority=5)
    medium = Task("add", [1, 2], priority=2)

    q.enqueue(low)
    q.enqueue(high)
    q.enqueue(medium)

    assert q.dequeue().priority == 5
    assert q.dequeue().priority == 2
    assert q.dequeue().priority == 0

def test_fifo_within_same_priority():
    q = TaskQueue()
    t1 = Task("add", [1, 2], priority=1)
    t2 = Task("add", [3, 4], priority=1)
    t3 = Task("add", [5, 6], priority=1)

    q.enqueue(t1)
    q.enqueue(t2)
    q.enqueue(t3)

    assert q.dequeue().task_id == t1.task_id
    assert q.dequeue().task_id == t2.task_id
    assert q.dequeue().task_id == t3.task_id

def test_is_empty():
    q = TaskQueue()
    assert q.is_empty()
    q.enqueue(Task("add", [1, 2]))
    assert not q.is_empty()

def test_size():
    q = TaskQueue()
    q.enqueue(Task("add", [1, 2]))
    q.enqueue(Task("add", [3, 4]))
    assert q.size() == 2