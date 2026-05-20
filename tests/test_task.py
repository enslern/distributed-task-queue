import pytest
from shared.task import Task


def test_task_default_status():
    t = Task("add", [1, 2])
    assert t.status == "pending"

def test_task_default_retry_count():
    t = Task("add", [1, 2])
    assert t.retry_count == 0

def test_task_default_args():
    t = Task("add")
    assert t.args == []

def test_task_unique_ids():
    t1 = Task("add", [1, 2])
    t2 = Task("add", [1, 2])
    assert t1.task_id != t2.task_id

def test_task_priority_stored():
    t = Task("add", [1, 2], priority=5)
    assert t.priority == 5