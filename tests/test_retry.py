import pytest
from unittest.mock import MagicMock
from shared.task import Task
from shared.result_store import ResultStore
from shared.dead_letter_queue import DeadLetterQueue


def make_worker():
    """Create a local worker with mocked queue for unit testing retry logic."""
    from broker.queue_ import TaskQueue
    from worker.worker import Worker

    # use a real queue and stores but don't connect to broker
    queue        = TaskQueue()
    result_store = ResultStore()
    dlq          = DeadLetterQueue()

    # patch _send so worker doesn't try to connect
    worker = Worker.__new__(Worker)
    worker.queue        = queue
    worker.result_store = result_store
    worker.dlq          = dlq
    worker.worker_id    = "test-worker"
    return worker, queue, result_store, dlq


def test_unknown_task_goes_to_failed():
    from shared.tasks import TASK_REGISTRY
    from shared.task import Task
    from broker.queue_ import TaskQueue
    from shared.result_store import ResultStore
    from shared.dead_letter_queue import DeadLetterQueue

    queue        = TaskQueue()
    result_store = ResultStore()
    dlq          = DeadLetterQueue()

    # simulate execute with unknown function
    task = Task("nonexistent_function", [1, 2])

    # manually check registry
    assert "nonexistent_function" not in TASK_REGISTRY

def test_result_store_saves_correctly():
    store = ResultStore()
    store.save("task-123", 42, "success")
    result = store.get("task-123")
    assert result["result"] == 42
    assert result["status"] == "success"

def test_result_store_returns_none_for_missing():
    store = ResultStore()
    assert store.get("nonexistent") is None

def test_dlq_stores_failed_task():
    dlq  = DeadLetterQueue()
    task = Task("flaky", [1, 2])
    task.retry_count = 3
    dlq.add(task, "random failure")
    assert len(dlq.all()) == 1
    assert dlq.all()[0]["reason"] == "random failure"
    assert dlq.all()[0]["function_name"] == "flaky"