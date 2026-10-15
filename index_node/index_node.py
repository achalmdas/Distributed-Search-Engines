"""
Index Node  (Backend / Data Service)
CECS 327 - Distributed Search Engine, Milestone 2

This process plays three roles from the report:
  1. search shard - answers search requests from the gateway   (request-reply)
  2. replica      - applies index updates sent to the group and acks them
                                                                (group communication)
  3. subscriber   - listens to the broker topic "index.updates" (pub-sub)

Design:
  - one main thread accepts connections and puts them on a queue
  - k worker threads take connections off the queue and handle them
    (thread-pool request server, slide 25)
  - one extra thread stays connected to the broker as a subscriber
  - Locks protect the shared counter and the document store (slide 14)
  - every message received/sent is logged with a timestamp and its id

Run:
    python index_node/index_node.py            # listens on port 8001
    python index_node/index_node.py 8002       # second node on port 8002

Message formats (one JSON object per line, newline-terminated):
  search   gateway -> node : {"msg_type": "search", "request_id": "1", "query": "tcp"}
           node -> gateway : {"msg_type": "results", "request_id": "1",
                              "node_id": "index-8001",
                              "results": [{"doc_id": 6, "title": "...", "score": 2}]}
  update   gateway -> node : {"msg_type": "index_update", "update_id": 1, "ts": 1.0,
                              "doc": {"doc_id": 16, "title": "...", "body": "..."}}
           node -> gateway : {"msg_type": "ack", "update_id": 1, "node_id": "index-8001",
                              "doc_count": 16, "digest": "a1b2c3d4", "status": "applied"}
  event    broker -> node  : {"msg_type": "publish", "topic": "index.updates",
                              "payload": {... an index_update ...}}
  error    node -> gateway : {"msg_type": "error", "request_id": "1",
                              "error": "...", "error_type": "bad_request"}
"""

import hashlib
import json
import logging
import os
import queue
import re
import socket
import sys
import threading
import time

# ---------------------------------------------------------------- config
HOST = "0.0.0.0"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
NODE_ID = f"index-{PORT}"
NUM_WORKERS = 4

BROKER_HOST = "127.0.0.1"
BROKER_PORT = 9000
TOPIC = "index.updates"
RECONNECT_SECONDS = 5

MAX_MESSAGE_BYTES = 1_000_000

# ---------------------------------------------------------------- logging

LOG_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "logs"
)

# Create the logs folder if it doesn't exist
os.makedirs(LOG_DIR, exist_ok=True)

# Each index node gets its own log file
LOG_FILE = os.path.join(
    LOG_DIR,
    f"{NODE_ID}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

log = logging.getLogger(NODE_ID)

# ---------------------------------------------------------------- shared state
DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "documents.json")

with open(DATA_FILE) as f:
    ALL_DOCS = json.load(f)

# Determine which shard this process owns based on its port.
# 8001 -> shard 0
# 8002 -> shard 1
# 8003 -> shard 2
SHARD_ID = PORT - 8001
NUM_SHARDS = 3

DOCS = [
    doc for doc in ALL_DOCS
    if doc["doc_id"] % NUM_SHARDS == SHARD_ID
]

log.info(
    "Loaded shard %d with %d documents: %s",
    SHARD_ID,
    len(DOCS),
    [doc["doc_id"] for doc in DOCS]
)
 
requests_handled = 0
counter_lock = threading.Lock()      # protects requests_handled (critical section)

docs_lock = threading.Lock()         # protects DOCS, holdback and next_update_id
next_update_id = 1                   # the update we are waiting for (total order)
holdback = {}                        # update_id -> doc, updates that arrived early

work_queue = queue.Queue()           # main thread to worker threads


# ---------------------------------------------------------------- marshalling
def marshal(message):
    """dict -> bytes on the wire (JSON text, UTF-8, one line)."""
    return (json.dumps(message) + "\n").encode("utf-8")


def unmarshal(data):
    """bytes or text off the wire -> dict."""
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    message = json.loads(data)
    if not isinstance(message, dict):
        raise ValueError("message must be a JSON object")
    return message


def read_message(conn):
    data = b""
    while not data.endswith(b"\n"):          # read until newline
        chunk = conn.recv(1024)
        if not chunk:
            break
        data += chunk
        if len(data) > MAX_MESSAGE_BYTES:
            raise ValueError("message too large")
    return unmarshal(data)


# ---------------------------------------------------------------- search
def score(query, doc):
    words = query.lower().split()
    text = re.findall(r"\w+", (doc["title"] + " " + doc["body"]).lower())  # counts how many times query words pop up in title or body
    return sum(text.count(w) for w in words)


def search(query):
    with docs_lock:                  # snapshot, so updates can keep arriving
        docs = list(DOCS)
    scored = [(score(query, d), d) for d in docs]
    scored = [(s, d) for s, d in scored if s > 0]
    scored.sort(key=lambda pair: pair[0], reverse=True)
    return [{"doc_id": d["doc_id"], "title": d["title"], "score": s}
            for s, d in scored[:10]]


# ---------------------------------------------------------------- index updates
def owns(doc):
    """True if this doc belongs to our shard (same rule used at startup)."""
    return doc["doc_id"] % NUM_SHARDS == SHARD_ID


def store_doc(doc):
    for i, existing in enumerate(DOCS):
        if existing["doc_id"] == doc["doc_id"]:
            DOCS[i] = doc
            return
    DOCS.append(doc)


def digest():
    """Short hash of our doc ids, so the gateway can compare replicas."""
    ids = ",".join(str(d["doc_id"]) for d in DOCS)
    return hashlib.md5(ids.encode()).hexdigest()[:8]


def apply_update(update):
    """
    Apply one index update and return what happened:
      applied   - it was next in line, so it and anything waiting went in
      buffered  - it arrived early, we hold it until its turn (total order)
      duplicate - we already have it, ignore it (reliable multicast resends)
      not_mine  - the doc belongs to another shard, so we only count it
    """
    global next_update_id
    update_id = update.get("update_id")
    doc = update.get("doc")
    if not isinstance(update_id, int) or not isinstance(doc, dict):
        raise ValueError("index_update needs an integer update_id and a doc")
    if not all(k in doc for k in ("doc_id", "title", "body")):
        raise ValueError("doc needs doc_id, title and body")

    with docs_lock:
        if update_id < next_update_id or update_id in holdback:
            return "duplicate"
        holdback[update_id] = doc
        status = "buffered"
        while next_update_id in holdback:
            ready = holdback.pop(next_update_id)
            if owns(ready):                       # every node keeps the order,
                store_doc(ready)                  # but only stores its own shard
            next_update_id += 1
            status = "applied"
        if status == "applied" and not owns(doc):
            status = "not_mine"
        return status


# ---------------------------------------------------------------- request handling
def handle_search(request):
    request_id = request.get("request_id")
    query = request.get("query")
    if not isinstance(request_id, str) or not request_id:
        raise ValueError("request_id must be a non-empty string")
    if not isinstance(query, str) or not query:
        raise ValueError("query must be a non-empty string")

    return {
        "msg_type": "results",
        "request_id": request_id,
        "node_id": NODE_ID,
        "results": search(query),
    }


def handle_index_update(update):
    status = apply_update(update)
    with docs_lock:
        doc_count, doc_digest = len(DOCS), digest()
    log.info("UPDATE update_id=%s status=%s doc_count=%d digest=%s",
             update.get("update_id"), status, doc_count, doc_digest)

    return {
        "msg_type": "ack",
        "update_id": update.get("update_id"),
        "node_id": NODE_ID,
        "doc_count": doc_count,
        "digest": doc_digest,
        "status": status,
    }


def handle(conn, addr):
    global requests_handled
    try:
        request = read_message(conn)
        log.info("RECEIVED from %s: %s", addr, request)

        msg_type = request.get("msg_type", "search")   # no msg_type means search
        if msg_type == "search":
            response = handle_search(request)
        elif msg_type == "index_update":
            response = handle_index_update(request)
        elif msg_type == "ping":
            response = {"msg_type": "pong", "node_id": NODE_ID}
        else:
            raise ValueError(f"unknown msg_type: {msg_type}")

        conn.sendall(marshal(response))
        log.info("SENT to %s: %s", addr, response.get("msg_type"))

        with counter_lock:                        # critical section
            requests_handled += 1

    except Exception as e:
        log.error("Error handling %s: %s", addr, e)
        try:
            conn.sendall(marshal({"msg_type": "error",
                                  "node_id": NODE_ID,
                                  "error": str(e),
                                  "error_type": type(e).__name__}))
        except OSError:
            pass                                  # client already gone
    finally:
        conn.close()


def worker():
    while True:
        conn, addr = work_queue.get()  # worker thread loop so it can take requests
        try:
            handle(conn, addr)
        finally:
            work_queue.task_done()


# ---------------------------------------------------------------- pub-sub subscriber
def subscribe_once():
    """Connect to the broker, subscribe, then read events until it goes away."""
    with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=RECONNECT_SECONDS) as sock:
        sock.sendall(marshal({"msg_type": "subscribe", "topic": TOPIC, "node_id": NODE_ID}))
        log.info("SUBSCRIBED to topic %s at %s:%d", TOPIC, BROKER_HOST, BROKER_PORT)

        sock.settimeout(None)
        for line in sock.makefile("r"):
            if not line.strip():
                continue
            event = unmarshal(line)
            log.info("EVENT on %s: %s", event.get("topic", TOPIC), event)

            update = event.get("payload", event)
            if update.get("msg_type") == "index_update":
                status = apply_update(update)      # ignores repeats from the multicast
                log.info("EVENT update_id=%s status=%s", update.get("update_id"), status)


def subscriber():
    """Keep trying to subscribe. The node still works if the broker is down."""
    warned = False
    while True:
        try:
            subscribe_once()
            warned = False
        except Exception as e:
            if not warned:
                log.warning("Broker unavailable (%s), retrying every %ds",
                            e, RECONNECT_SECONDS)
                warned = True
        time.sleep(RECONNECT_SECONDS)


# ---------------------------------------------------------------- main
def main():
    for _ in range(NUM_WORKERS):
        threading.Thread(target=worker, daemon=True).start()
    threading.Thread(target=subscriber, daemon=True).start()

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
