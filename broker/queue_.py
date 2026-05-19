from queue import PriorityQueue
import itertools

counter = itertools.count()


class TaskQueue:
    def __init__(self):
        self.queue = PriorityQueue()

    def enqueue(self, task):
        # negate priority — PriorityQueue pops lowest first
        self.queue.put((-task.priority, next(counter), task))

    def dequeue(self):
        _, _, task = self.queue.get()
        return task

    def is_empty(self):
        return self.queue.empty()

    def size(self):
        return self.queue.qsize()