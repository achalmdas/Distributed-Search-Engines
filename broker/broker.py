"""
Publish-Subscribe Broker
CECS 327 - Milestone 2

Publishers do not name receivers. Subscribers register interest in a topic and
the broker matches and delivers each event to every subscriber of that topic.

Run:  python3 broker/broker.py        (listens on 127.0.0.1:9000)

Messages (one JSON object per line):
    subscribe : {"msg_type": "subscribe", "topic": "index.updates", "subscriber_id": "replica-8011"}   (node_id is also accepted)
                -> {"msg_type": "subscribed", ...}   connection then stays open
    publish   : {"msg_type": "publish", "topic": "index.updates", "event_id": 1, "payload": {...}}
                -> {"msg_type": "publish_ack", "delivered": <n>}
    event     : {"msg_type": "event", "topic": ..., "event_id": ..., "payload": ...}  (pushed to subscribers)
    status    : {"msg_type": "broker_status"} -> {"msg_type": "broker_status_reply", "subscribers": {topic: n}}

Delivery guarantee: at-most-once (a subscriber that is offline misses events).
"""
import os
import socket
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.logger import get_logger
from common.protocol import ProtocolError, make_message, recv_msg, send_msg

HOST, PORT = "127.0.0.1", 9000
log = get_logger("broker-9000")

subscribers = {}                 # topic -> list of (subscriber_id, socket)
table_lock = threading.Lock()    # protects the subscriber table


def deliver(topic, event):
    with table_lock:
        targets = list(subscribers.get(topic, []))
    delivered = 0
    for sub_id, conn in targets:
        try:
            send_msg(conn, event)
            delivered += 1
            log.info("DELIVER event_id=%s topic=%s to %s", event["event_id"], topic, sub_id)
        except OSError:
            log.warning("subscriber %s unreachable, removing", sub_id)
            remove(topic, conn)
    return delivered


def remove(topic, conn):
    with table_lock:
        subscribers[topic] = [(i, c) for i, c in subscribers.get(topic, []) if c is not conn]


def handle(conn, addr):
    keep_open = False
    try:
        msg = recv_msg(conn)
        kind = msg.get("msg_type")
        if kind == "subscribe":
            topic, sub_id = msg["topic"], msg.get("subscriber_id") or msg.get("node_id") or str(addr)
            with table_lock:
                subscribers.setdefault(topic, []).append((sub_id, conn))
            send_msg(conn, make_message("subscribed", topic=topic, subscriber_id=sub_id))
            log.info("SUBSCRIBE %s -> topic=%s", sub_id, topic)
            keep_open = True
            while conn.recv(1024):          # block until the subscriber disconnects
                pass
            log.info("UNSUBSCRIBE %s (disconnected)", sub_id)
            remove(topic, conn)
        elif kind == "publish":
            topic = msg["topic"]
            log.info("PUBLISH event_id=%s topic=%s payload=%s", msg.get("event_id"), topic, msg.get("payload"))
            event = make_message("event", topic=topic, event_id=msg.get("event_id"), payload=msg.get("payload"))
            n = deliver(topic, event)
            send_msg(conn, make_message("publish_ack", event_id=msg.get("event_id"), delivered=n))
        elif kind == "broker_status":
            with table_lock:
                counts = {t: len(v) for t, v in subscribers.items()}
            send_msg(conn, make_message("broker_status_reply", subscribers=counts))
        else:
            send_msg(conn, make_message("error", error=f"unknown msg_type {kind!r}"))
    except (ProtocolError, KeyError, ConnectionError, OSError) as error:
        log.warning("error from %s: %s", addr, error)
    finally:
        if not keep_open:
            conn.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()
    log.info("Broker listening on %s:%d", HOST, PORT)
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()