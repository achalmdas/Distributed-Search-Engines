"""
End-to-end test of the peer-to-peer overlay (Milestone 3).

Starts real peer processes (python3 p2p/peer.py) on ports 9101-9105 and checks:
  1. four peers form a correct ring (successor / predecessor pointers, ring closes)
  2. documents published into the DHT end up on the peers that own their keys
  3. a lookup routed from any entry peer reaches the same, correct owner
  4. a search through the overlay returns the same ranking as scoring the documents directly,
     also when the normal client asks through the gateway (p2p_search, p2p_ring)
  5. a fifth peer joins: the ring is correct again and keys moved to the new peer
  6. a peer leaves gracefully: its keys are handed over and searches still work
  7. a peer crashes (killed): the ring repairs itself and replicas keep the data available
  8. a joining peer with an id that is already taken is refused

Run from the repository root with nothing running on ports 8000 and 9101-9109:
    python3 tests/test_p2p.py
"""
import json
import os
import subprocess
import sys
import time
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from common.protocol import make_message, request_reply
from p2p.peer import RING, hash_id, in_range, peer_id_for, tokenize

HOST = "127.0.0.1"
PEER_PY = os.path.join(ROOT, "p2p", "peer.py")
DOCS = json.load(open(os.path.join(ROOT, "index_node", "data", "documents.json"), encoding="utf-8"))
procs = {}
failures = 0


def check(condition, text):
    global failures
    print(("  PASS: " if condition else "  FAIL: ") + text)
    if not condition:
        failures += 1


def start(port, join=None, node_id=None):
    args = [sys.executable, PEER_PY, str(port)]
    if join:
        args += ["--join", str(join)]
    if node_id is not None:
        args += ["--id", str(node_id)]
    procs[port] = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    time.sleep(0.6)
    return procs[port]


def ask(port, msg_type, **fields):
    reply = request_reply(HOST, port, make_message(msg_type, **fields), 15)
    if reply.get("msg_type") == "error":
        raise RuntimeError(reply.get("error"))
    return reply


def wait_for(condition, timeout=15):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if condition():
                return True
        except (OSError, RuntimeError):
            pass
        time.sleep(0.5)
    return False


def walk_ring(entry):
    """Follow successor pointers from one peer. Returns the list of status replies in ring order."""
    members, seen, port = [], set(), entry
    for _ in range(20):
        s = ask(port, "status")
        if s["peer"]["id"] in seen:
            break
        seen.add(s["peer"]["id"])
        members.append(s)
        port = s["successor"]["port"]
    return members


def ring_ok(ports):
    """The ring walked from any peer visits exactly `ports`, in id order, with matching predecessors."""
    expected = sorted(ports, key=peer_id_for)
    for entry in ports:
        walked = walk_ring(entry)
        got = [s["peer"]["port"] for s in walked]
        if sorted(got) != sorted(expected) or len(got) != len(expected):
            return False
        i = got.index(expected[0])
        if got[i:] + got[:i] != expected:
            return False
        for s in walked:
            n = expected.index(s["peer"]["port"])
            if s["predecessor"] is None or s["predecessor"]["port"] != expected[n - 1]:
                return False
            if s["successor"]["port"] != expected[(n + 1) % len(expected)]:
                return False
    return True


def expected_owner(key, ports):
    key_id = hash_id(key)
    ids = sorted(peer_id_for(p) for p in ports)
    return next((i for i in ids if i >= key_id), ids[0])


def expected_search(query):
    words = list(dict.fromkeys(tokenize(query)))
    scores = Counter()
    for d in DOCS:
        counts = Counter(tokenize(d["title"] + " " + d["body"]))
        for w in words:
            if counts[w]:
                scores[d["doc_id"]] += counts[w]
    return sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))[:10]


def search_matches(port, query):
    r = ask(port, "search", query=query)
    return [(d["doc_id"], d["score"]) for d in r["results"]] == expected_search(query)


def stop_all():
    for p in procs.values():
        if p.poll() is None:
            p.kill()
    for p in procs.values():
        p.wait()


def main():
    global failures
    try:
        print("\n=== 1. Four peers start and form a ring")
        start(9105)
        start(9104, join=9105)
        start(9101, join=9104)
        start(9102, join=9101)
        ports = [9105, 9104, 9101, 9102]
        check(wait_for(lambda: ring_ok(ports)), "ring of 4 peers is correct from every entry point "
              f"(ids {sorted(peer_id_for(p) for p in ports)})")
        check(wait_for(lambda: all(len(ask(p, "status")["fingers"]) == 8 and
                                   all(f["node"] for f in ask(p, "status")["fingers"]) for p in ports)),
              "every peer built a full finger table")

        print("\n=== 2. Documents are published into the DHT")
        r = ask(9104, "publish", docs=DOCS)
        total_keys = r["keys"]
        check(r["documents"] == len(DOCS), f"published {r['documents']} documents as {total_keys} keys")
        check(len(r["placed"]) >= 3, f"keys spread over {len(r['placed'])} peers: {r['placed']}")
        statuses = {p: ask(p, "status") for p in ports}
        check(sum(s["keys_owned"] for s in statuses.values()) == total_keys,
              "every key is owned by exactly one peer")
        check(all(s["keys_total"] > s["keys_owned"] for s in statuses.values()),
              "every peer also holds replicas of its predecessor's keys")

        print("\n=== 3. Lookups routed through the overlay")
        owners = set()
        for entry in ports:
            r = ask(entry, "lookup", key="term:distributed")
            owners.add(r["owner"]["id"])
        check(len(owners) == 1, "the same owner is found from every entry peer")
        check(owners == {expected_owner("term:distributed", ports)},
              f"the owner is the first peer clockwise from key id {hash_id('term:distributed')}")
        far = max(ports, key=lambda p: (peer_id_for(p) - hash_id("term:systems")) % RING)
        r = ask(far, "lookup", key="term:systems")
        check(len(r["path"]) >= 1 and r["owner"]["id"] == expected_owner("term:systems", ports),
              f"lookup from peer {far} reached the owner in {len(r['path'])} hop(s)")

        print("\n=== 4. Resource discovery: search and documents found through peers")
        check(all(search_matches(p, "distributed systems") for p in ports),
              "search 'distributed systems' matches direct scoring from every entry peer")
        check(search_matches(9102, "inverted index search"), "multi-word search matches direct scoring")
        r = ask(9105, "search", query="zzzznotaword")
        check(r["results"] == [] and not r["lookups"][0]["found"], "an unknown word gives an empty result, not an error")
        d = ask(9101, "get_doc", doc_id=3)
        check(d["doc"] and d["doc"]["title"] == "Peer-to-Peer Networks",
              f"document 3 was discovered at peer {d['owner']['port']}")

        print("\n=== 4b. The normal client can use the overlay through the gateway")
        gw = subprocess.Popen([sys.executable, os.path.join(ROOT, "gateway", "gateway.py")],
                              stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        procs[8000] = gw
        time.sleep(1.5)
        reply = request_reply(HOST, 8000, {"request_id": "t1", "msg_type": "p2p_search",
                                           "query": "distributed systems"}, 15)
        got = [(d["doc_id"], d["score"]) for d in reply.get("results", [])]
        check(reply.get("msg_type") == "p2p_search_result" and got == expected_search("distributed systems"),
              "gateway p2p_search returns the same ranking as direct scoring")
        reply = request_reply(HOST, 8000, {"request_id": "t2", "msg_type": "p2p_ring"}, 15)
        check(sorted(m["port"] for m in reply.get("members", [])) == sorted(ports),
              f"gateway p2p_ring lists all {len(ports)} peers")
        reply = request_reply(HOST, 8000, {"request_id": "t3", "msg_type": "p2p_search", "query": "  "}, 15)
        check("error" in reply, "an empty p2p query is rejected")

        print("\n=== 5. A peer joins")
        before = ask(9102, "status")["keys_owned"]
        start(9103, join=9102)
        ports.append(9103)
        check(wait_for(lambda: ring_ok(ports)), f"ring of 5 peers is correct (new peer id {peer_id_for(9103)})")
        s = ask(9103, "status")
        check(s["keys_owned"] > 0, f"the new peer received {s['keys_owned']} keys from its successor")
        check(sum(ask(p, "status")["keys_owned"] for p in ports) == total_keys,
              "after the join every key is still owned by exactly one peer")
        check(all(search_matches(p, "distributed systems") for p in ports),
              "searches still return the right results from every peer")
        r = ask(9103, "lookup", key="term:distributed")
        check(r["owner"]["id"] == expected_owner("term:distributed", ports), "lookups use the new ring")

        print("\n=== 6. A peer leaves gracefully")
        leaving = 9101
        r = ask(leaving, "leave")
        check(r["msg_type"] == "left", f"peer {leaving} handed its keys over and left")
        time.sleep(0.8)
        ports.remove(leaving)
        check(wait_for(lambda: ring_ok(ports)), f"ring of {len(ports)} peers is correct without peer {leaving}")
        check(sum(ask(p, "status")["keys_owned"] for p in ports) == total_keys,
              "no key was lost: every key is owned by exactly one remaining peer")
        check(all(search_matches(p, "distributed systems") for p in ports),
              "searches still return the right results")

        print("\n=== 7. A peer crashes")
        time.sleep(4)                                    # let replicas settle after the leave
        victim = 9103
        procs[victim].kill()
        procs[victim].wait()
        ports.remove(victim)
        check(wait_for(lambda: ring_ok(ports), timeout=20),
              f"the ring repaired itself after peer {victim} was killed ({len(ports)} peers left)")
        check(wait_for(lambda: all(search_matches(p, "distributed systems") for p in ports)),
              "searches still return the right results (replicas on the successor)")
        check(wait_for(lambda: all(search_matches(p, "peer to peer replication") for p in ports)),
              "a second query is also complete")
        check(wait_for(lambda: sum(ask(p, "status")["keys_owned"] for p in ports) == total_keys),
              "all keys are owned again by the remaining peers")

        print("\n=== 8. A joining peer with an id that is already taken is refused")
        taken = peer_id_for(ports[0])
        p = start(9109, join=ports[0], node_id=taken)
        try:
            code = p.wait(timeout=8)
        except subprocess.TimeoutExpired:
            code = None
        check(code == 1, "the duplicate id was rejected and the peer exited")
        check(ring_ok(ports), "the ring is unchanged")
    finally:
        stop_all()

    print()
    if failures:
        print(f"{failures} CHECK(S) FAILED")
        sys.exit(1)
    print("ALL P2P TESTS PASSED")


if __name__ == "__main__":
    main()
