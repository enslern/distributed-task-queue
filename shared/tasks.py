import random
import time


def add(a, b):
    return a + b

def multiply(a, b):
    return a * b

def greet(name):
    return f"Hello, {name}!"

def flaky(a, b):
    if random.random() < 0.7:
        raise ValueError("random failure")
    return a + b

def slow(a, b):
    time.sleep(10)   # will trigger timeout
    return a + b


TASK_REGISTRY = {
    "add":      add,
    "multiply": multiply,
    "greet":    greet,
    "flaky":    flaky,
    "slow":     slow,
}
