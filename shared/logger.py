from datetime import datetime


class AppendLogger:
    def __init__(self, path):
        self.path = path

    def log(self, message):
        timestamp = datetime.now().isoformat()
        with open(self.path, "a") as f:
            f.write(f"[{timestamp}] {message}\n")


task_logger   = AppendLogger("tasks.log")
worker_logger = AppendLogger("workers.log")