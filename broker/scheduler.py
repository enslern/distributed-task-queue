from collections import deque


class FIFOScheduler:
    """Tasks execute in submission order within same priority."""
    def pick_worker(self, workers):
        for worker_id, info in workers.items():
            if info["active_task"] is None:
                return worker_id
        return None


class RoundRobinScheduler:
    """Distributes tasks evenly across workers in rotation."""
    def __init__(self):
        self._cycle = deque()

    def pick_worker(self, workers):
        available = [wid for wid, info in workers.items()
                     if info["active_task"] is None]
        if not available:
            return None

        for wid in available:
            if wid not in self._cycle:
                self._cycle.append(wid)

        for _ in range(len(self._cycle)):
            self._cycle.rotate(-1)
            candidate = self._cycle[-1]
            if candidate in available:
                return candidate
        return None


class LeastLoadedScheduler:
    """Assigns task to the worker with fewest active tasks."""
    def pick_worker(self, workers):
        available = {wid: info for wid, info in workers.items()
                     if info["active_task"] is None}
        if not available:
            return None
        return next(iter(available))


# Change this to swap scheduling policy
SCHEDULER = RoundRobinScheduler()