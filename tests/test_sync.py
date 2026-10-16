"""
End-to-end synchronization test (Milestone 2).

Starts the broker, 3 index shards, 3 replicas and the gateway, then checks:
  - an index update reaches every shard and every replica (group message),
    each member acks, and the digests prove the data matches
  - the pub-sub event reaches every subscriber
  - the new document is searchable through the gateway
  - changing an existing document changes the digests (and they still match)
  - a crashed shard and a crashed replica are reported, and are caught up
    with the updates they missed after they restart

Run from the repo root:
    python3 tests/test_sync.py
"""
import glob
import os
import subprocess
import sys
import time
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from common.protocol import make_message, request_reply

procs = {}


def start(key, *args):
    procs[key] = subprocess.Popen([sys.executable, *args], cwd=ROOT,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def stop(key):
    procs[key].kill()
    procs[key].wait()


def gw(msg):
    return request_reply("127.0.0.1", 8000, {"request_id": str(uuid.uuid4()), **msg}, 15)


def search(q):
    return gw({"query": q})["results"]


def update(doc_id, title, body):
    return gw({"msg_type": "update", "doc": {"doc_id": doc_id, "title": title, "body": body}})


def step(text):
    print(f"\n=== {text}")


def check(cond, text):
    print(("  PASS: " if cond else "  FAIL: ") + text)
    if not cond:
        raise SystemExit(1)


def all_ack(r):
    return all(v["status"] == "ack" for v in {**r["shards"], **r["replicas"]}.values())


def main():
    for f in glob.glob(os.path.join(ROOT, "logs", "*.log")):
        os.remove(f)
    try:
        start("broker", "broker/broker.py")
        time.sleep(0.5)
        for p in (8001, 8002, 8003):
            start(p, "index_node/index_node.py", str(p))
        for p in (8011, 8012, 8013):
            start(p, "replica/replica.py", str(p))
        time.sleep(1.5)
        start("gateway", "gateway/gateway.py")
        for _ in range(60):                                   # wait until all 6 subscribed
            try:
                n = request_reply("127.0.0.1", 9000, make_message("broker_status"))["subscribers"].get("index.updates", 0)
                if n == 6:
                    break
            except OSError:
                pass
            time.sleep(0.25)
        time.sleep(0.5)

        step("1. Baseline: everything starts in sync")
        st = gw({"msg_type": "status"})
        check(st["in_sync"], "replica digests match each other and each shard's digest")
        check(search("raft") == [], "term 'raft' not found before the update")

        step("2. One update -> all 3 shards + all 3 replicas + pub-sub event")
        r = update(16, "Raft Consensus", "Raft uses leader election and log replication for raft clusters")
        check(all_ack(r), "all 6 members acked")
        check(r["in_sync"], "digests match (replicas agree, shards agree with replicas)")
        check(r["event_delivered_to"] == 6, "pub-sub event delivered to 6 subscribers")
        check(any(x["doc_id"] == 16 for x in search("raft")), "new document is searchable through the gateway")
        st = gw({"msg_type": "status"})
        check(all(v["doc_count"] == 16 for v in st["replicas"].values()), "every replica holds 16 documents")
        check(sum(v["doc_count"] for v in st["shards"].values()) == 16, "shards together hold 16 documents")
        check(all(v["events_received"] == 1 for v in st["replicas"].values()), "every replica logged the event")

        step("3. Changing an existing document changes the digest, still in sync")
        before = gw({"msg_type": "status"})["replicas"]["8011"]["digest"]
        r = update(16, "Raft Consensus", "Completely rewritten body about quorum")
        after = gw({"msg_type": "status"})["replicas"]["8011"]["digest"]
        check(r["in_sync"] and before != after, f"digest changed ({before} -> {after}) and members still match")
        check(search("raft") == [] or all(x["doc_id"] != 16 or x["score"] < 3 for x in search("raft")), "old text is no longer indexed")

        step("4. Shard 8002 and replica 8013 crash; the group keeps working")
        stop(8002); stop(8013)
        time.sleep(0.5)
        r = update(17, "Paxos", "Paxos is a consensus protocol")
        check(r["shards"]["8002"]["status"] == "failed" and r["replicas"]["8013"]["status"] == "failed",
              "result reports 8002 and 8013 failed")
        check(r["replicas"]["8011"]["status"] == "ack" and r["shards"]["8003"]["status"] == "ack",
              "the other members still acked")
        check(not r["in_sync"], "result honestly reports the group is not fully in sync")

        step("5. Both restart and are caught up (missed updates replayed in order)")
        start(8002, "index_node/index_node.py", "8002")
        start(8013, "replica/replica.py", "8013")
        time.sleep(2)
        r = update(18, "Gossip Protocols", "Gossip spreads state between peers")
        check(all_ack(r), "all 6 members acked")
        check(r["in_sync"], "digests match: restarted members received updates 1 to 4")
        check(any(x["doc_id"] == 17 for x in search("paxos")), "the update made while 8002 and 8013 were down is searchable")
        st = gw({"msg_type": "status"})
        check(st["in_sync"], "status: every member is at update 5 and digests match")

        print("\nALL SYNC TESTS PASSED")
    finally:
        for p in procs.values():
            p.kill()


if __name__ == "__main__":
    main()