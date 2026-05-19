from queue import Queue

class TaskQueue:
    def __init__(self):
        self.queue=Queue()

    def enqueue(self,task):
        self.queue.put(task)

    def dequeue(self):
        return self.queue.get()

    def is_empty(self):
        return self.queue.empty()

    def size(self):
        return self.queue.qsize()