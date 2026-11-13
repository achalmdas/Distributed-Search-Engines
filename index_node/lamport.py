"""
Lamport logical clock kept by every index node.
CECS 327 - Milestone 4

Rules (Lamport, 1978):
    local event or send : L = L + 1           (the message carries L)
    receive (ts)        : L = max(L, ts) + 1

If event a happened before event b, then L(a) < L(b). Two events with the same
L are ordered by node id, which gives every node the same total order.
"""
import threading

clock_lock = threading.Lock()        # protects time (critical section)
time = 0


def tick():
    """Local event or message send."""
    global time
    with clock_lock:
        time += 1
        return time


def receive(timestamp):
    """Message received. A message without a timestamp counts as a local event."""
    global time
    with clock_lock:
        if isinstance(timestamp, int):
            time = max(time, timestamp)
        time += 1
        return time


def read():
    with clock_lock:
        return time
