import time
from client.client import Client

if __name__ == "__main__":
    client = Client()

    ids = [
        client.submit_task("greet", "low priority",    priority=0),
        client.submit_task("greet", "high priority",   priority=5),
        client.submit_task("greet", "medium priority", priority=2),
        client.submit_task("add",   2, 3),
        client.submit_task("multiply", 4, 5),
        client.submit_task("flaky", 1, 2),
    ]

    print(f"\nSubmitted {len(ids)} tasks. Waiting for results...\n")
    time.sleep(6)

    for task_id in ids:
        result = client.get_result(task_id)
        print(f"{task_id[:8]}... → {result}")

    print("\n── Metrics ──")
    print(client.get_metrics())