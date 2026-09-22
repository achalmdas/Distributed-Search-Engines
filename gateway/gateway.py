"""
Gateway  (Query Processor)
CECS 327 - Distributed Search Engine, Milestone 1

  - one main thread accepts client connections and puts them on a queue
  - k worker threads take connections off the queue and handle them
  - for each client query, the gateway fans the query out to every
    configured index node, waits for each response, and merges the
    results by score before replying to the client
  - if an index node is unreachable, the gateway logs it and continues
    with whatever nodes did respond (crash-failure assumption from the
    report's system model - a down node degrades the result set,
    it does not take down the whole gateway)
  - a Lock protects the shared request counter, same as index_node.py
  - every message sent/received (client<->gateway and gateway<->node)
    is logged

Run:
    python gateway/gateway.py                          # gateway on 7000, node on 8001
    python gateway/gateway.py 7000 localhost:8001 localhost:8002
"""

import os
import queue
import socket
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.logger import setup_logging
from common.protocol import send_json, recv_json, connect_with_retry

# ---------------------------------------------------------------- config
HOST = "0.0.0.0"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 7000
NUM_WORKERS = 4

raw_nodes = sys.argv[2:] if len(sys.argv) > 2 else ["localhost:8001"]
INDEX_NODES = []
for n in raw_nodes:
    h, p = n.split(":")
    INDEX_NODES.append((h, int(p)))

log = setup_logging("gateway")

# ---------------------------------------------------------------- shared state
requests_handled = 0
counter_lock = threading.Lock()   # protects requests_handled (critical section)

work_queue = queue.Queue()        # main thread -> worker threads


# ---------------------------------------------------------------- fan-out to index nodes
def query_node(host, port, request):
    """Send `request` to one index node and return its parsed response, or None on failure."""
    try:
        conn = connect_with_retry(host, port, attempts=2, delay=0.3, log=log)
    except ConnectionError as e:
        log.warning("Node %s:%d unreachable: %s", host, port, e)
        return None

    try:
        send_json(conn, request)
        log.info("SENT to node %s:%d: %s", host, port, request)
        response = recv_json(conn)
        log.info("RECV from node %s:%d: %s", host, port, response)
        return response
    except OSError as e:
        log.warning("Error talking to node %s:%d: %s", host, port, e)
        return None
    finally:
        conn.close()


def fan_out(request):
    merged = []
    nodes_queried, nodes_failed = [], []

    for host, port in INDEX_NODES:
        node_id = f"{host}:{port}"
        response = query_node(host, port, request)
        if response is None or "error" in response:
            nodes_failed.append(node_id)
            continue
        nodes_queried.append(response.get("node_id", node_id))
        merged.extend(response.get("results", []))

    merged.sort(key=lambda r: r["score"], reverse=True)
    return merged[:10], nodes_queried, nodes_failed


# ---------------------------------------------------------------- client request handling
def handle(conn, addr):
    global requests_handled
    try:
        request = recv_json(conn)
        if request is None:
            return
        log.info("RECEIVED from client %s: %s", addr, request)

        results, nodes_queried, nodes_failed = fan_out(request)

        response = {
            "request_id": request.get("request_id"),
            "results": results,
            "nodes_queried": nodes_queried,
            "nodes_failed": nodes_failed,
        }
        send_json(conn, response)
        log.info("SENT to client %s: request_id=%s results=%d nodes_failed=%s",
                  addr, response["request_id"], len(results), nodes_failed)

        with counter_lock:   # critical section
            requests_handled += 1

    except Exception as e:
        log.error("Error handling client %s: %s", addr, e)
        try:
            send_json(conn, {"error": str(e)})
        except OSError:
            pass
    finally:
        conn.close()


def worker():
    while True:
        conn, addr = work_queue.get()
        try:
            handle(conn, addr)
        finally:
            work_queue.task_done()


# ---------------------------------------------------------------- main
def main():
    for _ in range(NUM_WORKERS):
        threading.Thread(target=worker, daemon=True).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()
    log.info("gateway listening on %s:%d with %d workers, index nodes=%s",
              HOST, PORT, NUM_WORKERS, INDEX_NODES)

    while True:
        conn, addr = server.accept()
        work_queue.put((conn, addr))


if __name__ == "__main__":
    main()