import json

SUBMIT_TASK     = "SUBMIT_TASK"
TASK_RESULT     = "TASK_RESULT"
REQUEST_TASK    = "REQUEST_TASK"
TASK_ASSIGNED   = "TASK_ASSIGNED"
NO_TASK         = "NO_TASK"
ACK             = "ACK"
REGISTER_WORKER = "REGISTER_WORKER"
HEARTBEAT       = "HEARTBEAT"
GET_RESULT      = "GET_RESULT"
GET_METRICS     = "GET_METRICS"


def encode(message: dict) -> bytes:
    raw    = json.dumps(message)
    length = len(raw).to_bytes(4, byteorder="big")
    return length + raw.encode()


def decode(data: bytes) -> dict:
    return json.loads(data.decode())