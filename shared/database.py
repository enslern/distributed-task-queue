
DB_PATH = "taskqueue.db"


import sqlite3
import json



class Database:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.conn    = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_tables()

    def _init_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id       TEXT PRIMARY KEY,
                function_name TEXT NOT NULL,
                args          TEXT NOT NULL,
                status        TEXT NOT NULL,
                priority      INTEGER DEFAULT 0,
                retry_count   INTEGER DEFAULT 0,
                created_time  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS results (
                task_id    TEXT PRIMARY KEY,
                result     TEXT,
                status     TEXT NOT NULL,
                finished_at TEXT,
                FOREIGN KEY (task_id) REFERENCES tasks(task_id)
            );

            CREATE TABLE IF NOT EXISTS workers (
                worker_id      TEXT PRIMARY KEY,
                last_heartbeat REAL NOT NULL,
                active_task_id TEXT
            );
        """)
        self.conn.commit()

    # ── Tasks ─────────────────────────────────────────────────────────────────

    def insert_task(self, task):
        self.conn.execute("""
            INSERT INTO tasks (task_id, function_name, args, status, priority, retry_count, created_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            task.task_id,
            task.function_name,
            json.dumps(task.args),
            task.status,
            task.priority,
            task.retry_count,
            task.created_time.isoformat()
        ))
        self.conn.commit()

    def update_task_status(self, task_id, status, retry_count=None):
        if retry_count is not None:
            self.conn.execute(
                "UPDATE tasks SET status=?, retry_count=? WHERE task_id=?",
                (status, retry_count, task_id)
            )
        else:
            self.conn.execute(
                "UPDATE tasks SET status=? WHERE task_id=?",
                (status, task_id)
            )
        self.conn.commit()

    def get_unfinished_tasks(self):
        return self.conn.execute("""
            SELECT * FROM tasks
            WHERE status IN ('pending', 'running')
            ORDER BY priority DESC, created_time ASC
        """).fetchall()

    def get_all_tasks(self, limit=100):
        return self.conn.execute("""
            SELECT * FROM tasks ORDER BY created_time DESC LIMIT ?
        """, (limit,)).fetchall()

    def get_task_counts(self):
        rows = self.conn.execute("""
            SELECT status, COUNT(*) as count FROM tasks GROUP BY status
        """).fetchall()
        return {row["status"]: row["count"] for row in rows}

    # ── Results ───────────────────────────────────────────────────────────────

    def save_result(self, task_id, result, status):
        from datetime import datetime
        self.conn.execute("""
            INSERT INTO results (task_id, result, status, finished_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                result=excluded.result,
                status=excluded.status,
                finished_at=excluded.finished_at
        """, (task_id, json.dumps(result), status, datetime.now().isoformat()))
        self.conn.commit()

    def get_result(self, task_id):
        row = self.conn.execute(
            "SELECT * FROM results WHERE task_id=?", (task_id,)
        ).fetchone()
        if row:
            return {
                "result":      json.loads(row["result"]) if row["result"] else None,
                "status":      row["status"],
                "finished_at": row["finished_at"]
            }
        return None

    def get_throughput(self, seconds=60):
        """Tasks completed in last N seconds."""
        from datetime import datetime, timedelta
        since = (datetime.now() - timedelta(seconds=seconds)).isoformat()
        row = self.conn.execute("""
            SELECT COUNT(*) as count FROM results
            WHERE finished_at > ? AND status = 'success'
        """, (since,)).fetchone()
        return row["count"] if row else 0

    def get_failure_count(self, seconds=60):
        from datetime import datetime, timedelta
        since = (datetime.now() - timedelta(seconds=seconds)).isoformat()
        row = self.conn.execute("""
            SELECT COUNT(*) as count FROM results
            WHERE finished_at > ? AND status = 'failed'
        """, (since,)).fetchone()
        return row["count"] if row else 0

    # ── Workers ───────────────────────────────────────────────────────────────

    def upsert_worker(self, worker_id, last_heartbeat, active_task_id=None):
        self.conn.execute("""
            INSERT INTO workers (worker_id, last_heartbeat, active_task_id)
            VALUES (?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
                last_heartbeat=excluded.last_heartbeat,
                active_task_id=excluded.active_task_id
        """, (worker_id, last_heartbeat, active_task_id))
        self.conn.commit()

    def remove_worker(self, worker_id):
        self.conn.execute("DELETE FROM workers WHERE worker_id=?", (worker_id,))
        self.conn.commit()

    def get_all_workers(self):
        return self.conn.execute("SELECT * FROM workers").fetchall()