"""
End-to-end test of Milestone 4 (coordination between the index nodes).

Starts the 3 real index nodes (8001-8003) and the gateway and checks:
  1. the Lamport clock rules (tick on local events and sends, max + 1 on receive)
  2. the Bully election makes index-8003 the leader and every node agrees
  3. replies carry a logical timestamp that follows the receive rule
  4. mutual exclusion: 30 concurrent writes to the shared file with the lock keep all 30,
     and the critical sections never overlap in time
  5. the same race without the lock loses writes (the conflict the lock prevents)
  6. a search through the gateway makes all 3 shards update the shared file under the lock
  7. the gateway's coord_status reports the leader, the term and a logical timestamp
  8. the leader crashes: index-8002 is elected with a higher term
  9. the old leader comes back and takes over again (Bully)
 10. the leader crashes while the other nodes wait for the lock: no write is lost
 11. every received message has a clock value greater than its send timestamp

The test uses its own copy of the shared file, so shared/search_stats.json is not touched.
Run from the repository root with nothing running on ports 8000-8003:
    python3 tests/test_coordination.py
"""
import glob
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "index_node"))
SHARED = os.path.join(tempfile.mkdtemp(prefix="coord-test-"), "search_stats.json")
os.environ["COORD_SHARED_FILE"] = SHARED
from common.protocol import request_reply
import lamport

HOST = "127.0.0.1"
NODES = [8001, 8002, 8003]
LOG_DIR = os.path.join(ROOT, "logs")
procs = {}
offsets = {}
failures = 0


def check(condition, text):
    global failures
    print(("  PASS: " if condition else "  FAIL: ") + text)
    if not condition:
        failures += 1


def start(port):
    procs[port] = subprocess.Popen([sys.executable, os.path.join(ROOT, "index_node", "index_node.py"), str(port)],
                                   cwd=ROOT, env=os.environ.copy(),
                                   stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


def kill(port):
    procs[port].kill()
    procs[port].wait()


def ask(port, msg, timeout=60):
    return request_reply(HOST, port, msg, timeout)


def status(port):
    try:
        return ask(port, {"msg_type": "coord_status"}, 2)
    except (OSError, ValueError):
        return None


def leaders(ports):
    return {(status(p) or {}).get("leader") for p in ports}


def wait_for(condition, timeout=15):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if condition():
                return True
        except (OSError, ValueError, KeyError, TypeError):
            pass
        time.sleep(0.3)
    return False


def total():
    try:
        with open(SHARED, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {"total": 0, "history": []}


def race(ports, use_lock, count=10):
    done, threads = {}, []

    def one(p):
        try:
            done[p] = ask(p, {"msg_type": "stats_write", "count": count, "use_lock": use_lock}).get("writes")
        except (OSError, ValueError) as error:
            done[p] = f"error: {error}"

    for p in ports:
        threads.append(threading.Thread(target=one, args=(p,)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return done


def new_log_lines():
    out = []
    for path in glob.glob(os.path.join(LOG_DIR, "index-80*.log")):
        with open(path, encoding="utf-8") as f:
            f.seek(offsets.get(path, 0))
            out += [(os.path.basename(path), l.rstrip()) for l in f]
    return out


def stop_all():
    for p in procs.values():
        if p.poll() is None:
            p.kill()
    for p in procs.values():
        p.wait()


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    for path in glob.glob(os.path.join(LOG_DIR, "index-80*.log")):
        offsets[path] = os.path.getsize(path)
    try:
        print("\n=== 1. Lamport clock rules")
        lamport.time = 5
        a, b, c = lamport.tick(), lamport.receive(3), lamport.receive(20)
        check((a, b, c) == (6, 7, 21), f"tick 5 -> {a}; receive ts 3 -> {b} (max(6,3)+1); receive ts 20 -> {c} (max(7,20)+1)")

        print("\n=== 2. The index nodes start; the Bully election picks the highest port")
        for p in NODES:
            start(p)
            time.sleep(0.8)
        check(wait_for(lambda: leaders(NODES) == {8003}), "every node agrees that index-8003 is the leader")
        terms = {status(p)["term"] for p in NODES}
        check(len(terms) == 1 and terms.pop() >= 1, "every node knows the same election term")

        print("\n=== 3. Replies carry logical timestamps")
        r = ask(8002, {"msg_type": "coord_status", "sender_id": "test", "lamport_ts": 100000}, 2)
        check(r.get("lamport_ts", 0) > 100000,
              f"a message stamped 100000 makes the reply's timestamp larger ({r.get('lamport_ts')})")

        print("\n=== 4. Mutual exclusion on the shared file")
        before = total()["total"]
        done = race(NODES, True, 10)
        after = total()["total"]
        check(sum(done.values()) == 30 and after - before == 30, f"30 locked writes, total grew by {after - before}")
        spans, open_at = [], {}
        for name, line in sorted(new_log_lines(), key=lambda x: x[1][:23]):
            m = re.match(r"(\S+ \S+) \[index-(\d+)\] \w+: \[L=\d+\] (ENTER|EXIT) critical section", line)
            if not m:
                continue
            t = time.mktime(time.strptime(m.group(1)[:19], "%Y-%m-%d %H:%M:%S")) + int(m.group(1)[20:]) / 1000
            if m.group(3) == "ENTER":
                open_at[m.group(2)] = t
            elif m.group(2) in open_at:
                spans.append((open_at.pop(m.group(2)), t))
        spans.sort()
        overlap = any(spans[i + 1][0] < spans[i][1] for i in range(len(spans) - 1))
        check(len(spans) >= 30 and not overlap, f"the {len(spans)} critical sections never overlapped in time")

        print("\n=== 5. The same race without the lock")
        before = total()["total"]
        race(NODES, False, 10)
        after = total()["total"]
        check(after - before < 30, f"without the lock {30 - (after - before)} of 30 writes were lost")

        print("\n=== 6. A search through the gateway updates the shared file under the lock")
        procs[8000] = subprocess.Popen([sys.executable, os.path.join(ROOT, "gateway", "gateway.py")], cwd=ROOT,
                                       env=os.environ.copy(), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        time.sleep(2.5)
        before = total()["total"]
        reply = request_reply(HOST, 8000, {"request_id": "s1", "query": "distributed systems",
                                           "sender_id": "test", "lamport_ts": 5}, 15)
        check(len(reply.get("results", [])) > 0 and isinstance(reply.get("lamport_ts"), int) and reply["lamport_ts"] > 5,
              f"the search returned results with a gateway timestamp ({reply.get('lamport_ts')})")
        check(wait_for(lambda: total()["total"] - before == 3, 5), "all 3 shards recorded the search")

        print("\n=== 7. coord_status through the gateway")
        st = request_reply(HOST, 8000, {"request_id": "c1", "msg_type": "coord_status"}, 15)
        check(st.get("msg_type") == "coord_status_result" and st["leader"].get("id") == "index-8003"
              and isinstance(st.get("lamport_ts"), int),
              f"the gateway reports leader {st.get('leader', {}).get('id')}, term {st.get('leader', {}).get('term')}")
        old_term = st["leader"].get("term")

        print("\n=== 8. The leader crashes")
        kill(8003)
        live = [8001, 8002]
        check(wait_for(lambda: leaders(live) == {8002}), "the remaining nodes elect index-8002")
        check(status(8002)["term"] > old_term, f"with a higher term ({status(8002)['term']} > {old_term})")
        done = race(live, True, 5)
        check(sum(v for v in done.values() if isinstance(v, int)) == 10, "the lock works with the new leader")

        print("\n=== 9. The old leader comes back and takes over again (Bully)")
        start(8003)
        check(wait_for(lambda: leaders(NODES) == {8003}), "index-8003 is the leader again")

        print("\n=== 10. The leader crashes while the others wait for the lock")
        before = total()
        result = {}
        t = threading.Thread(target=lambda: result.update(race(NODES, True, 20)))
        t.start()
        time.sleep(0.5)
        kill(8003)
        t.join(timeout=90)
        after = total()
        ok = {p: v for p, v in result.items() if isinstance(v, int)}
        check(ok.get(8001) == 20 and ok.get(8002) == 20, f"index-8001 and index-8002 finished all their writes: {ok}")
        from_8003 = sum(1 for h in after["history"] if h["node"] == "index-8003"
                        and h["total_after"] > before["total"])
        grew = after["total"] - before["total"]
        check(grew == 40 + from_8003, f"no write was lost: total grew by {grew} = 40 + {from_8003} written by "
              f"index-8003 before it crashed")
        check(leaders(live) == {8002}, "index-8002 took over")
        recovered = [l for _, l in new_log_lines() if "LOCK STATE recovered" in l and "index-8002" in l]
        check(bool(recovered), "the new leader rebuilt the lock state from the other nodes")

        print("\n=== 11. Event ordering")
        recv = []
        for _, line in new_log_lines():
            m = re.search(r"\[L=(\d+)\] RECEIVED from .*'lamport_ts': (\d+)", line)
            if m:
                recv.append((int(m.group(1)), int(m.group(2))))
        check(recv and all(L > ts for L, ts in recv),
              f"all {len(recv)} received timestamped messages are later than their send timestamps")
    finally:
        stop_all()

    print()
    if failures:
        print(f"{failures} CHECK(S) FAILED")
        sys.exit(1)
    print("ALL COORDINATION TESTS PASSED")


if __name__ == "__main__":
    main()
