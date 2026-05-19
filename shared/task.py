from datetime import datetime
import uuid

import uuid
from datetime import datetime


class Task:
    def __init__(self, function_name, args=None, kwargs=None, priority=0):
        self.task_id       = str(uuid.uuid4())
        self.function_name = function_name
        self.args          = args if args is not None else []
        self.kwargs        = kwargs if kwargs is not None else {}
        self.status        = "pending"
        self.priority      = priority
        self.retry_count   = 0
        self.created_time  = datetime.now()

    def __repr__(self):
        return (f"Task(id={self.task_id[:8]}..., "
                f"fn={self.function_name!r}, "
                f"status={self.status!r}, "
                f"priority={self.priority})")