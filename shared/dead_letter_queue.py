class DeadLetterQueue:
    def __init__(self):
        self.failed_tasks = []

    def add(self, task, reason):
        self.failed_tasks.append({
            "task_id":       task.task_id,
            "function_name": task.function_name,
            "args":          task.args,
            "retry_count":   task.retry_count,
            "reason":        reason
        })
        print(f"[DLQ] Task dead → {task.function_name}{tuple(task.args)} | reason: {reason}")

    def all(self):
        return self.failed_tasks