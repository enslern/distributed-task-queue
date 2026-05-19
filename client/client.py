import socket
from shared import protocol


class Client:
    def __init__(self, host="localhost", port=9999):
        self.host = host
        self.port = port

    def _send(self, message: dict) -> dict:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            s.sendall(protocol.encode(message))
            header = s.recv(4)
            length = int.from_bytes(header, byteorder="big")
            raw = b""
            while len(raw) < length:
                chunk = s.recv(length - len(raw))
                if not chunk:
                    break
                raw += chunk
            return protocol.decode(raw)

    def submit_task(self, function_name, *args, priority=0):
        response = self._send({
            "type":          protocol.SUBMIT_TASK,
            "function_name": function_name,
            "args":          list(args),
            "priority":      priority
        })
        task_id = response.get("task_id", "")
        print(f"[CLIENT] Submitted → task_id={task_id[:8]}...")
        return task_id

    def get_result(self, task_id):
        return self._send({
            "type":    protocol.GET_RESULT,
            "task_id": task_id
        })

    def get_metrics(self):
        return self._send({"type": protocol.GET_METRICS})
