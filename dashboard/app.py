import asyncio
import json
import socket
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from shared import protocol

app = FastAPI(title="TaskQueue Dashboard")

BROKER_HOST = "localhost"
BROKER_PORT = 9999


def get_metrics() -> dict:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((BROKER_HOST, BROKER_PORT))
            s.sendall(protocol.encode({"type": protocol.GET_METRICS}))
            header = s.recv(4)
            length = int.from_bytes(header, byteorder="big")
            raw = b""
            while len(raw) < length:
                chunk = s.recv(length - len(raw))
                if not chunk:
                    break
                raw += chunk
            return protocol.decode(raw)
    except Exception as e:
        return {"error": str(e)}


# ── REST API ──────────────────────────────────────────────────────────────────

@app.get("/metrics")
def metrics():
    return get_metrics()


@app.get("/tasks")
def tasks():
    from shared.database import Database
    db   = Database()
    rows = db.get_all_tasks(limit=50)
    return [dict(row) for row in rows]


@app.get("/workers")
def workers():
    from shared.database import Database
    db   = Database()
    rows = db.get_all_workers()
    return [dict(row) for row in rows]


# ── Dashboard HTML ────────────────────────────────────────────────────────────

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>TaskQueue Dashboard</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Courier New', monospace;
            background: #0d1117;
            color: #e6edf3;
            padding: 24px;
        }
        h1 {
            font-size: 22px;
            color: #58a6ff;
            margin-bottom: 24px;
            letter-spacing: 2px;
            text-transform: uppercase;
        }
        .cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 16px;
            margin-bottom: 32px;
        }
        .card {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 20px;
        }
        .card .label {
            font-size: 11px;
            color: #8b949e;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 8px;
        }
        .card .value {
            font-size: 32px;
            font-weight: bold;
            color: #58a6ff;
        }
        .card .value.green  { color: #3fb950; }
        .card .value.red    { color: #f85149; }
        .card .value.yellow { color: #d29922; }
        .charts {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 16px;
            margin-bottom: 16px;
        }
        .chart-box {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 20px;
        }
        .chart-box h2 {
            font-size: 12px;
            color: #8b949e;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 16px;
        }
        .status {
            display: inline-block;
            width: 8px; height: 8px;
            border-radius: 50%;
            background: #3fb950;
            margin-right: 6px;
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50%       { opacity: 0.4; }
        }
        .uptime {
            font-size: 12px;
            color: #8b949e;
            margin-bottom: 24px;
        }
        .badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: bold;
            margin: 4px;
        }
        .badge.success { background: #1a4a1a; color: #3fb950; }
    </style>
</head>
<body>
    <h1><span class="status"></span>TaskQueue Dashboard</h1>
    <div class="uptime" id="uptime">Connecting to broker...</div>

    <div class="cards">
        <div class="card">
            <div class="label">Queue Size</div>
            <div class="value yellow" id="queue_size">—</div>
        </div>
        <div class="card">
            <div class="label">Active Workers</div>
            <div class="value" id="active_workers">—</div>
        </div>
        <div class="card">
            <div class="label">Total Submitted</div>
            <div class="value" id="total_submitted">—</div>
        </div>
        <div class="card">
            <div class="label">Success</div>
            <div class="value green" id="total_success">—</div>
        </div>
        <div class="card">
            <div class="label">Failed</div>
            <div class="value red" id="total_failed">—</div>
        </div>
        <div class="card">
            <div class="label">TPS (60s)</div>
            <div class="value green" id="throughput">—</div>
        </div>
    </div>

    <div class="charts">
        <div class="chart-box">
            <h2>Throughput (tasks/min)</h2>
            <canvas id="tpsChart"></canvas>
        </div>
        <div class="chart-box">
            <h2>Failures (60s)</h2>
            <canvas id="failChart"></canvas>
        </div>
    </div>

    <div class="chart-box">
        <h2>Workers</h2>
        <div id="workers_list" style="padding-top:8px">—</div>
    </div>

    <script>
        const MAX_POINTS = 30;

        const tpsChart = new Chart(document.getElementById('tpsChart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Tasks/min',
                    data: [],
                    borderColor: '#58a6ff',
                    backgroundColor: 'rgba(88,166,255,0.1)',
                    tension: 0.4,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: {
                    x: { ticks: { color: '#8b949e' }, grid: { color: '#21262d' } },
                    y: { ticks: { color: '#8b949e' }, grid: { color: '#21262d' }, beginAtZero: true }
                }
            }
        });

        const failChart = new Chart(document.getElementById('failChart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Failures',
                    data: [],
                    borderColor: '#f85149',
                    backgroundColor: 'rgba(248,81,73,0.1)',
                    tension: 0.4,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: {
                    x: { ticks: { color: '#8b949e' }, grid: { color: '#21262d' } },
                    y: { ticks: { color: '#8b949e' }, grid: { color: '#21262d' }, beginAtZero: true }
                }
            }
        });

        function addPoint(chart, label, value) {
            chart.data.labels.push(label);
            chart.data.datasets[0].data.push(value);
            if (chart.data.labels.length > MAX_POINTS) {
                chart.data.labels.shift();
                chart.data.datasets[0].data.shift();
            }
            chart.update();
        }

        function formatUptime(seconds) {
            const h = Math.floor(seconds / 3600);
            const m = Math.floor((seconds % 3600) / 60);
            const s = Math.floor(seconds % 60);
            return `Uptime: ${h}h ${m}m ${s}s`;
        }

        async function fetchMetrics() {
            try {
                const response = await fetch('/metrics');
                const d = await response.json();
                const now = new Date().toLocaleTimeString();

                if (d.error) {
                    document.getElementById('uptime').textContent = 'Broker unreachable: ' + d.error;
                    return;
                }

                document.getElementById('queue_size').textContent     = d.queue_size ?? '—';
                document.getElementById('active_workers').textContent  = d.active_workers ?? '—';
                document.getElementById('total_submitted').textContent = d.total_submitted ?? '—';
                document.getElementById('total_success').textContent   = d.total_success ?? '—';
                document.getElementById('total_failed').textContent    = d.total_failed ?? '—';
                document.getElementById('throughput').textContent      = d.throughput_60s ?? '—';
                document.getElementById('uptime').textContent          = formatUptime(d.uptime_seconds ?? 0);

                addPoint(tpsChart, now, d.throughput_60s ?? 0);
                addPoint(failChart, now, d.failures_60s ?? 0);

                const workers = d.worker_ids ?? [];
                document.getElementById('workers_list').innerHTML = workers.length === 0
                    ? '<span style="color:#8b949e">No workers connected</span>'
                    : workers.map(w =>
                        `<span class="badge success">${w}...</span>`
                      ).join('');

            } catch(e) {
                document.getElementById('uptime').textContent = 'Error: ' + e;
            }
        }

        fetchMetrics();
        setInterval(fetchMetrics, 2000);
    </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML


def main():
    uvicorn.run("dashboard.app:app", host="localhost", port=8000, reload=False)


if __name__ == "__main__":
    main()