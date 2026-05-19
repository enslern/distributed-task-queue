import time
import socket
from shared import protocol


class AsyncResult:
    def __init__(self, task_id, host="localhost", port=9999):
        self.task_id = task_id
        self.host    = host
        self.port    = port

    def _query(self) -> dict:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            s.sendall(protocol.encode({
                "type":    protocol.GET_RESULT,
                "task_id": self.task_id
            }))
            header = s.recv(4)
            length = int.from_bytes(header, byteorder="big")
            raw    = s.recv(length)
            return protocol.decode(raw)

    def get_status(self) -> str:
        return self._query().get("status", "unknown")

    def get_result(self):
        response = self._query()
        if response.get("status") == "success":
            return response.get("result")
        return None

    def wait_for_result(self, timeout=30, poll_interval=0.5):
        start = time.time()
        while True:
            response = self._query()
            status   = response.get("status")
            if status == "success":
                return response.get("result")
            elif status == "failed":
                raise Exception(f"Task failed: {response.get('result')}")
            elif time.time() - start > timeout:
                raise TimeoutError(f"Task {self.task_id[:8]}... timed out after {timeout}s")
            time.sleep(poll_interval)