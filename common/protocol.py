"""
common/protocol.py
CECS 327 - Distributed Search Engine, Milestone 1

Shared wire protocol used by every service: one JSON object per line,
newline-terminated, sent over a plain TCP socket. This matches the
format already implemented inline in index_node.py - gateway.py and
client.py use these helpers instead of duplicating that code.
"""

import json
import socket
import time


def send_json(conn, obj):
    conn.sendall((json.dumps(obj) + "\n").encode())


def recv_json(conn):

    data = b""
    while not data.endswith(b"\n"):
        chunk = conn.recv(4096)
        if not chunk:
            return None
        data += chunk
    return json.loads(data.decode())


def connect_with_retry(host, port, attempts=5, delay=0.5, log=None):
    """
    Try to connect to (host, port), retrying a few times.
    Used so the gateway/client don't just crash if a service has not
    finished starting up yet (an example would be when run_all.sh starts everything
    at roughly the same time).
    """
    last_err = None
    for i in range(attempts):
        try:
            return socket.create_connection((host, port), timeout=5)
        except OSError as e:
            last_err = e
            if log:
                log.info("Connect to %s:%d failed (%s), retry %d/%d",
                          host, port, e, i + 1, attempts)
            time.sleep(delay)
    raise ConnectionError(f"Could not connect to {host}:{port}: {last_err}")