import asyncio
import json
import time

from broker.queue_ import TaskQueue
from broker.scheduler import SCHEDULER
from shared.result_store import ResultStore
from shared.dead_letter_queue import DeadLetterQueue
from shared.database import Database
from shared.logger import task_logger, worker_logger
from shared.task import Task
from shared import protocol

HOST              = "localhost"
PORT              = 9999
HEARTBEAT_TIMEOUT = 10


class BrokerServer:
    def __init__(self):
        self.queue        = TaskQueue()
        self.result_store = ResultStore()
        self.dlq          = DeadLetterQueue()
        self.db           = Database()
        self.workers      = {}
        self.metrics      = {
            "total_submitted": 0,
            "total_success":   0,
            "total_failed":    0,
            "start_time":      time.time()
        }

    async def _recover_tasks(self):
        rows = self.db.get_unfinished_tasks()
        if not rows:
            print("[BROKER] No tasks to recover")
            return
        for row in rows:
            task             = Task(row["function_name"], json.loads(row["args"]), priority=row["priority"])
            task.task_id     = row["task_id"]
            task.status      = "pending"
            task.retry_count = row["retry_count"]
            self.queue.enqueue(task)
            self.db.update_task_status(task.task_id, "pending")
        print(f"[BROKER] Recovered {len(rows)} tasks")

    async def handle_connection(self, reader, writer):
        addr = writer.get_extra_info("peername")
        print(f"[BROKER] Connected: {addr}")
        try:
            while True:
                header   = await reader.readexactly(4)
                length   = int.from_bytes(header, byteorder="big")
                raw      = await reader.readexactly(length)
                msg      = protocol.decode(raw)
                response = await self.handle_message(msg)
                writer.write(protocol.encode(response))
                await writer.drain()
        except asyncio.IncompleteReadError:
            print(f"[BROKER] Disconnected: {addr}")
        except Exception as e:
            print(f"[BROKER] Error: {e}")
        finally:
            writer.close()

    async def handle_message(self, msg: dict) -> dict:
        msg_type = msg.get("type")

        if msg_type == protocol.SUBMIT_TASK:
            task = Task(
                function_name=msg["function_name"],
                args=msg.get("args", []),
                priority=msg.get("priority", 0)
            )
            self.queue.enqueue(task)
            self.db.insert_task(task)
            self.metrics["total_submitted"] += 1
            task_logger.log(f"SUBMITTED task_id={task.task_id} fn={task.function_name}")
            print(f"[BROKER] Queued → {task}")
            return {"type": protocol.ACK, "task_id": task.task_id}


        elif msg_type == protocol.REQUEST_TASK:

            worker_id = msg.get("worker_id")

            if worker_id and worker_id in self.workers:
                self.workers[worker_id]["active_task"] = None

            if not self.queue.is_empty():

                task = self.queue.dequeue()

                if worker_id and worker_id in self.workers:
                    self.workers[worker_id]["active_task"] = task.task_id

                self.db.update_task_status(task.task_id, "running")

                print(f"[BROKER] Assigned → {task} to {worker_id[:8] if worker_id else 'unknown'}...")

                return {

                    "type": protocol.TASK_ASSIGNED,

                    "task_id": task.task_id,

                    "function_name": task.function_name,

                    "args": task.args

                }

            return {"type": protocol.NO_TASK}

        elif msg_type == protocol.TASK_RESULT:
            task_id = msg["task_id"]
            result  = msg["result"]
            status  = msg["status"]
            self.result_store.save(task_id, result, status)
            self.db.save_result(task_id, result, status)
            self.db.update_task_status(task_id, status)
            if status == "success":
                self.metrics["total_success"] += 1
            else:
                self.metrics["total_failed"] += 1
            task_logger.log(f"RESULT task_id={task_id} status={status}")
            print(f"[BROKER] Result saved → {task_id[:8]}... status={status}")
            return {"type": protocol.ACK}

        elif msg_type == protocol.GET_RESULT:
            task_id = msg["task_id"]
            result  = self.db.get_result(task_id)
            if result:
                return {"type": protocol.ACK, "status": result["status"], "result": result["result"]}
            return {"type": protocol.ACK, "status": "pending", "result": None}

        elif msg_type == protocol.GET_METRICS:
            uptime     = time.time() - self.metrics["start_time"]
            throughput = self.db.get_throughput(60)
            failures   = self.db.get_failure_count(60)
            counts     = self.db.get_task_counts()
            return {
                "type":             protocol.ACK,
                "queue_size":       self.queue.size(),
                "active_workers":   len(self.workers),
                "worker_ids":       [wid[:8] for wid in self.workers],
                "total_submitted":  self.metrics["total_submitted"],
                "total_success":    self.metrics["total_success"],
                "total_failed":     self.metrics["total_failed"],
                "throughput_60s":   throughput,
                "failures_60s":     failures,
                "task_counts":      counts,
                "uptime_seconds":   round(uptime, 1)
            }

        elif msg_type == protocol.REGISTER_WORKER:
            worker_id = msg["worker_id"]
            self.workers[worker_id] = {"last_heartbeat": time.time(), "active_task": None}
            self.db.upsert_worker(worker_id, time.time())
            worker_logger.log(f"REGISTERED worker_id={worker_id}")
            print(f"[BROKER] Worker registered: {worker_id[:8]}...")
            return {"type": protocol.ACK}

        elif msg_type == protocol.HEARTBEAT:
            worker_id = msg["worker_id"]
            if worker_id in self.workers:
                self.workers[worker_id]["last_heartbeat"] = time.time()
                self.db.upsert_worker(worker_id, time.time(),
                                      self.workers[worker_id]["active_task"])
            return {"type": protocol.ACK}

        return {"type": "ERROR", "message": f"unknown type: {msg_type}"}

    async def monitor_workers(self):
        while True:
            now = time.time()
            for worker_id, info in list(self.workers.items()):
                elapsed = now - info["last_heartbeat"]
                if elapsed > HEARTBEAT_TIMEOUT:
                    print(f"[BROKER] Worker DEAD: {worker_id[:8]}... ({elapsed:.1f}s)")
                    worker_logger.log(f"DEAD worker_id={worker_id}")
                    active = info.get("active_task")
                    if active:
                        rows = self.db.get_unfinished_tasks()
                        for row in rows:
                            if row["task_id"] == active:
                                task             = Task(row["function_name"], json.loads(row["args"]), priority=row["priority"])
                                task.task_id     = row["task_id"]
                                task.retry_count = row["retry_count"]
                                self.queue.enqueue(task)
                                self.db.update_task_status(task.task_id, "pending")
                                print(f"[BROKER] Requeued lost task: {active[:8]}...")
                    self.db.remove_worker(worker_id)
                    del self.workers[worker_id]
            await asyncio.sleep(3)

    async def start(self):
        await self._recover_tasks()
        server = await asyncio.start_server(self.handle_connection, HOST, PORT)
        print(f"[BROKER] Running on {HOST}:{PORT}")
        async with server:
            asyncio.create_task(self.monitor_workers())
            await server.serve_forever()


def main():
    broker = BrokerServer()
    asyncio.run(broker.start())


if __name__ == "__main__":
    main()