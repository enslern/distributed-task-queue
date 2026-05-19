import socket
import time
import uuid
import threading
from concurrent.futures import ProcessPoolExecutor, TimeoutError

from shared import protocol
from shared.tasks import TASK_REGISTRY

MAX_RETRIES   = 3
TASK_TIMEOUT  = 5
MAX_WORKERS   = 4
POLL_INTERVAL = 1


def run_task(function_name, args):
    """Top-level — must be outside class to be picklable."""
    return TASK_REGISTRY[function_name](*args)

def recv_exact(sock, n):
    data = b""

    while len(data) < n:
        chunk = sock.recv(n - len(data))

        if not chunk:
            raise ConnectionError(
                "Socket closed before receiving full data"
            )

        data += chunk

    return data

class Worker:
    def __init__(self, host="localhost", port=9999):
        self.worker_id = str(uuid.uuid4())
        self.host      = host
        self.port      = port
        self.executor  = ProcessPoolExecutor(max_workers=MAX_WORKERS)

    def _send(self, message: dict) -> dict:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            s.sendall(protocol.encode(message))
            header = s.recv(4)
            length = int.from_bytes(header, byteorder="big")
            # read until we have all bytes
            raw = b""
            while len(raw) < length:
                chunk = s.recv(length - len(raw))
                if not chunk:
                    break
                raw += chunk
            return protocol.decode(raw)

    def request_task(self):
        return self._send({
            "type":      protocol.REQUEST_TASK,
            "worker_id": self.worker_id
        })

    def send_result(self, task_id, result, status):
        try:
            self._send({
                "type": protocol.TASK_RESULT,
                "task_id": task_id,
                "result": result,
                "status": status
            })
        except Exception as e:
            print(f"[WORKER] Failed to send result: {e}")

    def execute(self, task_data):
        function_name = task_data["function_name"]
        args          = task_data["args"]
        task_id       = task_data["task_id"]
        retry_count   = task_data.get("retry_count", 0)

        if function_name not in TASK_REGISTRY:
            self.send_result(task_id, None, "failed")
            print(f"[WORKER] Unknown task: {function_name}")
            return

        try:
            future = self.executor.submit(run_task, function_name, args)
            result = future.result(timeout=TASK_TIMEOUT)
            self.send_result(task_id, result, "success")
            print(f"[WORKER] {function_name}{tuple(args)} = {result}")

        except TimeoutError:
            retry_count += 1
            reason = f"timeout after {TASK_TIMEOUT}s"
            print(f"[WORKER] Timeout → {function_name} | attempt {retry_count}/{MAX_RETRIES}")
            self._handle_failure(task_id, function_name, args, retry_count, reason)

        except Exception as e:
            retry_count += 1
            print(f"[WORKER] Failed → {function_name} | attempt {retry_count}/{MAX_RETRIES} | {e}")
            self._handle_failure(task_id, function_name, args, retry_count, str(e))

    def _handle_failure(self, task_id, function_name, args, retry_count, reason):
        if retry_count < MAX_RETRIES:
            delay = 2 ** retry_count
            print(f"[WORKER] Retrying in {delay}s → {function_name}")
            time.sleep(delay)
            self._send({
                "type":          protocol.SUBMIT_TASK,
                "function_name": function_name,
                "args":          args,
                "priority":      0,
                "retry_count":   retry_count
            })
        else:
            self.send_result(task_id, reason, "failed")
            print(f"[WORKER] Exhausted retries → {function_name}")

    def _heartbeat_loop(self):
        while True:
            try:
                self._send({
                    "type":      protocol.HEARTBEAT,
                    "worker_id": self.worker_id
                })
            except Exception:
                pass
            time.sleep(3)

    def start(self):
        self._send({
            "type":      protocol.REGISTER_WORKER,
            "worker_id": self.worker_id
        })
        print(f"[WORKER] Registered | id={self.worker_id[:8]}...")

        t = threading.Thread(target=self._heartbeat_loop, daemon=True)
        t.start()

        while True:
            try:
                response = self.request_task()
                if response["type"] == protocol.TASK_ASSIGNED:
                    self.execute(response)
                else:
                    time.sleep(POLL_INTERVAL)
            except Exception as e:
                print(f"[WORKER] Connection error: {e} — retrying in 3s")
                time.sleep(3)

    def shutdown(self):
        self.executor.shutdown(wait=True)


def main():
    worker = Worker()
    worker.start()


if __name__ == "__main__":
    main()