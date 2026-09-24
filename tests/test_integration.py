"""
tests/test_integration.py
CECS 327 - Distributed Search Engine, Milestone 1

End-to-end smoke test: starts all 3 index_node shards and the gateway
as real subprocesses, sends one real query over a real socket directly
to the gateway (bypassing the interactive client.py, since it reads
from stdin), and checks the response is well-formed. Also verifies the
system keeps working if one shard is killed mid-run.

This does NOT assert on specific document content, since the sample
corpus in index_node/data/documents.json may change. It checks the
*process wiring and fault tolerance*, which is what Milestone 1 is
meant to demonstrate.

Run from the repo root:
    python3 tests/test_integration.py
"""

import json
import socket
import subprocess
import sys
import time
import uuid
import os

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 8000


def start(cmd):
    return subprocess.Popen(
        cmd, cwd=REPO_ROOT,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


def send_query(query, timeout=5):
    s = socket.create_connection((GATEWAY_HOST, GATEWAY_PORT), timeout=timeout)
    request = {"request_id": str(uuid.uuid4()), "query": query}
    s.sendall((json.dumps(request) + "\n").encode("utf-8"))

    data = b""
    while not data.endswith(b"\n"):
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    s.close()

    if not data:
        raise ConnectionError("no response from gateway")
    return request, json.loads(data.decode("utf-8"))


def main():
    procs = []
    try:
        print("Starting index_node shards on 8001, 8002, 8003...")
        for port in (8001, 8002, 8003):
            procs.append(start([sys.executable, "index_node/index_node.py", str(port)]))
        time.sleep(1.5)

        print("Starting gateway on 8000...")
        procs.append(start([sys.executable, "gateway/gateway.py"]))
        time.sleep(1.5)

        # --- basic round trip ---
        request, response = send_query("distributed systems")
        print("Client sent:", request)
        print("Client received:", response)

        assert response is not None, "Gateway sent no response"
        assert "error" not in response, f"Gateway returned an error: {response.get('error')}"
        assert response["request_id"] == request["request_id"], "request_id did not round-trip"
        assert isinstance(response.get("results"), list), "results field missing or not a list"
        assert len(response["results"]) > 0, (
            "No results returned - is index_node data/documents.json populated?"
        )
        print("PASS: basic round trip works.\n")

        # --- fault tolerance: kill one shard, confirm system still responds ---
        print("Killing shard on port 8002 to test fault tolerance...")
        procs[1].terminate()
        time.sleep(1)

        request2, response2 = send_query("distributed systems")
        print("Client received after shard failure:", response2)
        assert "error" not in response2, "Gateway failed entirely when one shard went down"
        assert isinstance(response2.get("results"), list)
        print("PASS: system still responds after one shard is killed.\n")

        print("ALL TESTS PASSED")

    finally:
        for p in procs:
            p.terminate()
        time.sleep(0.5)
        for p in procs:
            try:
                p.kill()
            except Exception:
                pass


if __name__ == "__main__":
    main()