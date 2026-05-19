class ResultStore:
    def __init__(self):
        self.results = {}

    def save(self, task_id, result, status):
        self.results[task_id] = {"result": result, "status": status}

    def get(self, task_id):
        return self.results.get(task_id)

    def all(self):
        return self.results