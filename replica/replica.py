"""
Replica node (member of the replica group)
CECS 327 - Milestone 2

Each replica keeps a FULL copy of the document set and applies index updates
sent by the coordinator (the gateway). It also subscribes to the broker topic
"index.updates" and logs every event it receives.

Run:  python3 replica/replica.py 8011      (also 8012, 8013)

Messages (one JSON object per line):
    index_update : {"msg_type":"index_update","update_id":N,"doc":{doc_id,title,body}}
        -> ack  : {"msg_type":"ack","update_id":N,"node_id":..,"doc_count":..,"digest":..,"last_update_id":N}
        -> nack : {"msg_type":"nack","last_update_id":L,...}   (gap: update N is not L+1)
    status       : {"msg_type":"status"} -> {"msg_type":"status_reply", doc_count, digest, last_update_id, events_received}
    search       : {"request_id":..,"query":..} -> {"request_id":..,"node_id":..,"results":[...]}

Updates are applied in update_id order and applying the same id twice is ignored
(idempotent). State is in memory, so a restarted replica starts from the
original documents and is caught up by the coordinator.
"""
import json
import os
import re
import socket
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from common.logger import get_logger
from common.protocol import (ProtocolError, docs_digest, make_message,
                             recv_msg, send_msg)

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8011
NODE_ID = f"replica-{PORT}"
BROKER = ("127.0.0.1", 9000)
TOPIC = "index.updates"
log = get_logger(NODE_ID)

with open(os.path.join(ROOT, "index_node", "data", "documents.json")) as f:
    DOCS = {d["doc_id"]: d for d in json.load(f)}

state_lock = threading.Lock()
last_update_id = 0
events_received = 0


def score(query, doc):
    words = query.lower().split()
    text = re.findall(r"\w+", (doc["title"] + " " + doc["body"]).lower())
    return sum(text.count(w) for w in words)


def search(query):
    with state_lock:
        docs = list(DOCS.values())
    scored = [(score(query, d), d) for d in docs]
    scored = sorted([(s, d) for s, d in scored if s > 0], key=lambda p: p[0], reverse=True)
    return [{"doc_id": d["doc_id"], "title": d["title"], "score": s} for s, d in scored[:10]]


def snapshot():
    """Call with state_lock NOT held."""
    with state_lock:
        docs = list(DOCS.values())
        shard_digests = {str(k): docs_digest([d for d in docs if d["doc_id"] % 3 == k]) for k in range(3)}
        return dict(node_id=NODE_ID, doc_count=len(docs), digest=docs_digest(docs),
                    shard_digests=shard_digests, last_update_id=last_update_id,
                    next_update_id=last_update_id + 1)


def apply_update(msg):
    global last_update_id
    uid, doc = msg["update_id"], msg["doc"]
    with state_lock:
        if uid <= last_update_id:
            kind = "ack"                                   # duplicate: already applied
            log.info("DUPLICATE update_id=%d ignored", uid)
        elif uid == last_update_id + 1:
            DOCS[doc["doc_id"]] = doc
            last_update_id = uid
            kind = "ack"
            log.info("APPLIED update_id=%d doc_id=%s", uid, doc["doc_id"])
        else:
            kind = "nack"                                  # gap: ask coordinator to replay
            log.warning("GAP: got update_id=%d but last applied is %d", uid, last_update_id)
    return make_message(kind, update_id=uid, **snapshot())


def handle(conn, addr):
    try:
        msg = recv_msg(conn)
        log.info("RECEIVED %s from %s", {k: msg[k] for k in msg if k != "doc"} | ({"doc_id": msg["doc"]["doc_id"]} if "doc" in msg else {}), addr)
        kind = msg.get("msg_type", "search")
        if kind == "index_update":
            reply = apply_update(msg)
        elif kind == "status":
            reply = make_message("status_reply", events_received=events_received, **snapshot())
        else:
            reply = {"request_id": msg.get("request_id"), "node_id": NODE_ID,
                     "results": search(msg.get("query", ""))}
        send_msg(conn, reply)
        log.info("SENT %s to %s", reply.get("msg_type", "search_reply"), addr)
    except (ProtocolError, KeyError, ConnectionError, OSError) as error:
        log.error("error handling %s: %s", addr, error)
        try:
            send_msg(conn, make_message("error", error=str(error)))
        except OSError:
            pass
    finally:
        conn.close()


def subscriber_loop():
    """Subscribe to the broker topic and log every event; reconnect if needed."""
    global events_received
    while True:
        try:
            with socket.create_connection(BROKER, timeout=2) as s:
                send_msg(s, make_message("subscribe", topic=TOPIC, subscriber_id=NODE_ID))
                s.settimeout(None)
                recv_msg(s)                                # "subscribed"
                log.info("SUBSCRIBED to topic %s", TOPIC)
                buf = s.makefile("rb")
                for line in buf:
                    event = json.loads(line.decode("utf-8"))
                    with state_lock:
                        events_received += 1
                    log.info("EVENT received event_id=%s payload=%s", event.get("event_id"), event.get("payload"))
        except (OSError, ProtocolError, ValueError):
            pass
        time.sleep(1)


def main():
    threading.Thread(target=subscriber_loop, daemon=True).start()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", PORT))
    server.listen()
    log.info("%s listening on port %d with %d documents", NODE_ID, PORT, len(DOCS))
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()