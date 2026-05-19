from client.client import Client
from shared.tasks import TASK_REGISTRY

_client = Client()


class TaskProxy:
    def __init__(self, func, priority=0, retries=3):
        self.func     = func
        self.priority = priority
        self.retries  = retries
        self.__name__ = func.__name__

    def __call__(self, *args, **kwargs):
        # direct call — runs locally
        return self.func(*args, **kwargs)

    def delay(self, *args):
        from client.result import AsyncResult
        task_id = _client.submit_task(self.__name__, *args, priority=self.priority)
        return AsyncResult(task_id)

    def apply_async(self, args=None, priority=None):
        from client.result import AsyncResult
        task_id = _client.submit_task(
            self.__name__,
            *(args or []),
            priority=priority if priority is not None else self.priority
        )
        return AsyncResult(task_id)


def task(priority=0, retries=3):
    def decorator(func):
        TASK_REGISTRY[func.__name__] = func
        return TaskProxy(func, priority=priority, retries=retries)
    return decorator