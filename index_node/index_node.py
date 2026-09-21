"""
Index Node  (Backend / Data Service)
CECS 327 - Distributed Search Engine, Milestone 1
 
 
Design:
  - one main thread accepts connections and puts them on a queue
  - k worker threads take connections off the queue and handle them
    (thread-pool request server, slide 25)
  - a Lock protects the shared request counter (slide 14)
  - every message received/sent is logged
 
Run:
    python index_node/index_node.py            # listens on port 8001
    python index_node/index_node.py 8002       # second node on port 8002
 
Message format (one JSON object per line, newline-terminated):
    gateway -> node :  {"request_id": "1", "query": "distributed systems"}
    node -> gateway :  {"request_id": "1", "node_id": "index-8001",
                        "results": [{"doc_id": 1, "title": "...", "score": 3}, ...]}
"""
 
import json
import logging
import os
import queue
import re
import socket
import sys
import threading
 
# ---------------------------------------------------------------- config
HOST = "0.0.0.0"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
NODE_ID = f"index-{PORT}"
NUM_WORKERS = 4
 
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger(NODE_ID)
 
# ---------------------------------------------------------------- shared state
DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "documents.json")
with open(DATA_FILE) as f:
    DOCS = json.load(f)
log.info("Loaded %d documents", len(DOCS))
 
requests_handled = 0                 
counter_lock = threading.Lock()      # protects requests_handled (critical section)
 
work_queue = queue.Queue()           # main thread to worker threads
 
 
# ---------------------------------------------------------------- search
def score(query, doc):
    words = query.lower().split()
    text = re.findall(r"\w+", (doc["title"] + " " + doc["body"]).lower()) # counts how many times query words pop up in title or body
    return sum(text.count(w) for w in words)
 
 
def search(query):
    scored = [(score(query, d), d) for d in DOCS]
    scored = [(s, d) for s, d in scored if s > 0]
    scored.sort(key=lambda pair: pair[0], reverse=True)
    return [{"doc_id": d["doc_id"], "title": d["title"], "score": s}
            for s, d in scored[:10]]
 
 
# ---------------------------------------------------------------- request handling
def handle(conn, addr):
    global requests_handled
    try:
        data = b""
        while not data.endswith(b"\n"):          # read until newline
            chunk = conn.recv(1024)
            if not chunk:
                break
            data += chunk
 
        request = json.loads(data.decode())
        log.info("RECEIVED from %s: %s", addr, request)
 
        response = {
            "request_id": request.get("request_id"),
            "node_id": NODE_ID,
            "results": search(request.get("query", "")),
        }
 
        conn.sendall((json.dumps(response) + "\n").encode())
        log.info("SENT to %s: request_id=%s results=%d",
                 addr, response["request_id"], len(response["results"]))
 
        with counter_lock:                        # critical section
            requests_handled += 1
 
    except Exception as e:
        log.error("Error handling %s: %s", addr, e)
        try:
            conn.sendall((json.dumps({"error": str(e)}) + "\n").encode())
        except OSError:
            pass                                  # client already gone
    finally:
        conn.close()
 
 
def worker():
    while True:
        conn, addr = work_queue.get() # worker thread loop so it can take requests
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
    log.info("%s listening on %s:%d with %d workers", NODE_ID, HOST, PORT, NUM_WORKERS)
 
    while True:                                   # main thread accept and enqueue
        conn, addr = server.accept()
        work_queue.put((conn, addr))
 
 
if __name__ == "__main__":
    main()
 