import pytest
import threading
import time
import asyncio
from broker.server import BrokerServer
from client.client import Client
from worker.worker import Worker


def start_broker():
    """Start broker in background thread."""
    broker = BrokerServer()
    loop   = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(broker.start())


def start_worker():
    worker = Worker(host="localhost", port=9999)
    worker.start()


@pytest.fixture(scope="module", autouse=True)
def broker_and_worker():
    """Start broker and worker once for all integration tests."""
    bt = threading.Thread(target=start_broker, daemon=True)
    bt.start()
    time.sleep(0.5)   # let broker start

    wt = threading.Thread(target=start_worker, daemon=True)
    wt.start()
    time.sleep(0.5)   # let worker register

    yield   # run tests

    # daemon threads die automatically


def wait_for_result(client, task_id, timeout=10, interval=0.2):
    """
    Poll until the task leaves 'pending' state or timeout is reached.
    Returns the final result dict.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = client.get_result(task_id)
        if result["status"] != "pending":
            return result
        time.sleep(interval)
    return client.get_result(task_id)   # return whatever we have on timeout


def test_submit_and_get_result():
    client  = Client()
    task_id = client.submit_task("add", 2, 3)
    result  = wait_for_result(client, task_id)
    assert result["status"] == "success"
    assert result["result"] == 5


def test_priority_respected():
    client = Client()
    low    = client.submit_task("add", 1, 1, priority=0)
    high   = client.submit_task("add", 2, 2, priority=5)
    result_low  = wait_for_result(client, low)
    result_high = wait_for_result(client, high)
    assert result_high["status"] == "success"
    assert result_low["status"]  == "success"


def test_unknown_task_fails():
    client  = Client()
    task_id = client.submit_task("nonexistent", 1, 2)
    result  = wait_for_result(client, task_id)
    assert result["status"] == "failed"


def test_multiple_tasks():
    client  = Client()
    ids     = [client.submit_task("add", i, i) for i in range(5)]
    results = [wait_for_result(client, tid) for tid in ids]
    assert all(r["status"] == "success" for r in results)
    assert [r["result"] for r in results] == [0, 2, 4, 6, 8]