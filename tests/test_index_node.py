"""
Tests for one index node (Milestone 2): updates, acks, ordering, duplicates,
and events from the broker. Uses a stand-in broker on port 9000, so do NOT run
the real broker at the same time.

Start one node first, then run this file from the repo root:
    python3 index_node/index_node.py 8001
    python3 tests/test_index_node.py

Node 8001 owns shard 0 (doc_id % 3 == 0).
"""
import json
import socket
import threading
import time

NODE_PORT = 8001
BROKER_PORT = 9000
subscribers = []


def ask(message):
    sock = socket.create_connection(("127.0.0.1", NODE_PORT), timeout=5)
    sock.sendall((json.dumps(message) + "\n").encode())
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    sock.close()
    return json.loads(data)


def broker():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", BROKER_PORT))
    server.listen()
    while True:
        conn, _ = server.accept()
        conn.recv(4096)
        subscribers.append(conn)


def publish(payload, raw=None):
    line = raw if raw is not None else json.dumps(
        {"msg_type": "publish", "topic": "index.updates", "payload": payload}) + "\n"
    for conn in subscribers:
        conn.sendall(line.encode())


def update(update_id, doc_id, title, body):
    return {"msg_type": "index_update", "update_id": update_id, "ts": time.time(),
            "doc": {"doc_id": doc_id, "title": title, "body": body}}


def check(cond, text):
    print(("  PASS: " if cond else "  FAIL: ") + text)
    if not cond:
        raise SystemExit(1)


def search(q):
    return ask({"msg_type": "search", "request_id": "t", "query": q})["results"]


def main():
    threading.Thread(target=broker, daemon=True).start()
    for _ in range(30):
        if subscribers:
            break
        time.sleep(1)
    check(subscribers, "node subscribed to the broker")

    print("\n1. search")
    check(any(r["doc_id"] == 6 for r in search("tcp")), "search 'tcp' finds doc 6")

    print("\n2. updates: early arrival is buffered, then applied in order")
    r = ask(update(2, 18, "Multicast", "A multicast sends one message to every member of a group."))
    check(r["status"] == "buffered" and r["next_update_id"] == 1, "update 2 arrives early -> buffered, node still needs 1")
    check(search("multicast") == [], "buffered update is not searchable yet")
    r = ask(update(1, 16, "Message Brokers", "A broker forwards each event to every subscriber."))
    check(r["next_update_id"] == 3, "update 1 arrives -> both applied, node now needs 3")
    check(any(x["doc_id"] == 18 for x in search("multicast")), "doc 18 (shard 0) is searchable")
    check(search("brokers") == [], "doc 16 belongs to another shard, so this node does not store it")
    r = ask(update(1, 16, "Message Brokers", "A broker forwards each event to every subscriber."))
    check(r["status"] == "duplicate", "resent update 1 -> duplicate")

    print("\n3. digest covers contents, not just ids")
    d1 = ask({"msg_type": "status"})["digest"]
    ask(update(3, 18, "Multicast", "Completely different text about quorum."))
    d2 = ask({"msg_type": "status"})["digest"]
    check(d1 != d2, "same doc_id, new body -> digest changes")

    print("\n4. events from the broker")
    publish(update(4, 21, "Event Driven Services", "Subscribers react to events without the publisher knowing them."))
    time.sleep(1)
    check(any(x["doc_id"] == 21 for x in search("subscribers")), "published update 4 was applied")
    publish(None, raw="this is not json\n")
    publish(None, raw=json.dumps({"msg_type": "publish", "payload": {"msg_type": "index_update"}}) + "\n")
    publish(update(5, 24, "Still Subscribed", "The node survived bad events."))
    time.sleep(1)
    check(any(x["doc_id"] == 24 for x in search("survived")), "bad events were skipped and the next good one applied")

    print("\n5. errors")
    check("non-empty" in ask({"msg_type": "search", "request_id": "4", "query": ""})["error"], "empty query -> error")
    check("unknown" in ask({"msg_type": "banana"})["error"], "unknown type -> error")
    print("\nALL INDEX NODE TESTS PASSED")


if __name__ == "__main__":
    main()