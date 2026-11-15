"""
Command line tool for Milestone 4 (coordination between the index nodes 8001-8003).

    python3 scripts/coord_cli.py status                every index node: leader, term, Lamport clock, lock
    python3 scripts/coord_cli.py --show-wire status    the same, printing the messages with their timestamps
    python3 scripts/coord_cli.py race                  every node writes the shared file 10 times at once, WITH the lock
    python3 scripts/coord_cli.py race --unsafe         the same race WITHOUT the lock (shows lost updates)
    python3 scripts/coord_cli.py stats                 the shared file: total, per node, last writes
    python3 scripts/coord_cli.py crash 8003            node 8003 stops with no warning (failure test)
    python3 scripts/coord_cli.py crash-leader          whichever node leads stops with no warning
    python3 scripts/coord_cli.py events --last 30      log lines of all nodes merged in Lamport order
    python3 scripts/coord_cli.py events --grep "ELECTION|LEADER"

Run from the repository root while the index nodes are running.
"""
import argparse
import glob
import json
import os
import re
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from common.protocol import ProtocolError, marshal, request_reply

HOST = "127.0.0.1"
NODES = [8001, 8002, 8003]
SHARED_FILE = os.environ.get("COORD_SHARED_FILE", os.path.join(ROOT, "shared", "search_stats.json"))
LOG_DIR = os.path.join(ROOT, "logs")
SHOW_WIRE = False
LINE = re.compile(r"^(\d{4}-\d\d-\d\d [\d:,]+) \[index-(\d+)\] \w+: \[L=(\d+)\] (.*)$")


def ask(port, msg, timeout=60):
    msg = dict(msg, sender_id="coord-cli", lamport_ts=0)
    if SHOW_WIRE:
        print(f"\n--- to index-{port} ---\n{marshal(msg)}")
    reply = request_reply(HOST, port, msg, timeout)
    if SHOW_WIRE:
        print(f"--- reply from index-{port} (logical timestamp {reply.get('lamport_ts')}) ---\n{marshal(reply)}")
    return reply


def statuses():
    out = {}
    for p in NODES:
        try:
            out[p] = ask(p, {"msg_type": "coord_status"}, timeout=2)
        except (OSError, ValueError, ProtocolError):
            out[p] = None
    return out


def show_status():
    print(f"\n  {'node':<11} {'clock':<6} {'leader':<11} {'term':<5} lock")
    leaders = set()
    for p, s in statuses().items():
        if s is None:
            print(f"  index-{p:<5} {'-':<6} {'-':<11} {'-':<5} (down)")
            continue
        leaders.add(s.get("leader"))
        lock = "holding the lock" if s.get("holding") else "waiting for the lock" if s.get("waiting") else ""
        if s.get("leader") == p:
            holder = s.get("lock_holder")
            lock = (lock + "  " if lock else "") + \
                f"[coordinator] holder={'index-' + str(holder) if holder else 'none'} " \
                f"queue={['index-' + str(q) for q in s.get('lock_queue', [])]}"
        leader = f"index-{s['leader']}" if s.get("leader") else "none"
        print(f"  index-{p:<5} {s.get('lamport_ts'):<6} {leader:<11} {s.get('term', '-')!s:<5} {lock}")
    agreed = len(leaders) == 1 and None not in leaders
    print(f"\n  all live nodes agree on the leader: {agreed}" + (f" (index-{leaders.pop()})" if agreed else ""))


def read_stats():
    try:
        with open(SHARED_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {"total": 0, "per_node": {}, "history": []}


def show_stats(last=8):
    data = read_stats()
    print(f"\nShared file {os.path.relpath(SHARED_FILE, ROOT)}: total = {data['total']}, per node = {data['per_node']}")
    for h in data["history"][-last:]:
        print(f"  L={h['lamport_ts']:<6} {h['node']}  total_after={h['total_after']:<5} query={h['query']!r}")


def race(unsafe, count):
    live = [p for p, s in statuses().items() if s is not None]
    before = read_stats()["total"]
    print(f"\nRace: nodes {['index-' + str(p) for p in live]} each write the shared file {count} times at once, "
          + ("WITHOUT the lock" if unsafe else "WITH the distributed lock"))
    done, threads = {}, []

    def one(p):
        try:
            done[p] = ask(p, {"msg_type": "stats_write", "count": count, "use_lock": not unsafe}).get("writes", 0)
        except (OSError, ValueError, ProtocolError) as error:
            done[p] = f"error: {error}"

    for p in live:
        threads.append(threading.Thread(target=one, args=(p,)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    writes = sum(v for v in done.values() if isinstance(v, int))
    after = read_stats()["total"]
    for p in live:
        print(f"  index-{p} finished {done.get(p)} writes")
    print(f"\n  writes made: {writes}, total went from {before} to {after} (+{after - before})")
    crashed = [p for p, v in done.items() if not isinstance(v, int)]
    if crashed:
        print(f"  ({', '.join('index-' + str(p) for p in crashed)} crashed before replying, so the writes it made "
              f"before the crash are in the file but not counted above)")
    if after - before < writes:
        print(f"  LOST UPDATES: {writes - (after - before)} writes were overwritten by another node")
    else:
        print("  no lost update: every write is in the file")
    return writes, after - before


def show_events(last, grep, since):
    rows = []
    for path in glob.glob(os.path.join(LOG_DIR, "index-80*.log")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                m = LINE.match(line.rstrip())
                if m:
                    rows.append((int(m.group(3)), int(m.group(2)), m.group(1), m.group(4)))
    if since:
        rows = [r for r in rows if r[2][11:] >= since]
    # happened-before check: every received message has a clock value above its send timestamp
    recv = [(L, int(t.group(1))) for L, _, _, text in rows
            for t in [re.search(r"'lamport_ts': (\d+)", text)] if text.startswith("RECEIVED") and t]
    ok = all(L > ts for L, ts in recv)
    rows.sort(key=lambda r: (r[0], r[1]))                 # Lamport order, node port breaks ties
    if grep:
        words = [w.lower() for w in grep.split("|") if w]
        rows = [r for r in rows if any(w in r[3].lower() for w in words)]
    shown = rows[-last:] if last else rows
    print(f"\nLog lines of all index nodes in Lamport order (timestamp, then node): {len(shown)} of {len(rows)}")
    for L, port, _, text in shown:
        print(f"  L={L:<6} index-{port}  {text[:150]}")
    print(f"\n  happened-before check: every one of the {len(recv)} received timestamped messages has a "
          f"clock value greater than its send timestamp: {ok}")
    return ok


def main():
    global SHOW_WIRE
    parser = argparse.ArgumentParser(description="Milestone 4 coordination tool")
    parser.add_argument("--show-wire", action="store_true", help="print the messages with their logical timestamps")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    p = sub.add_parser("race"); p.add_argument("--unsafe", action="store_true"); p.add_argument("--count", type=int, default=10)
    p = sub.add_parser("stats"); p.add_argument("--last", type=int, default=8)
    p = sub.add_parser("crash"); p.add_argument("port", type=int)
    sub.add_parser("crash-leader")
    p = sub.add_parser("events"); p.add_argument("--last", type=int, default=0); p.add_argument("--grep", default="")
    p.add_argument("--since", default="", help="only lines after this time of day, e.g. 14:05:00")
    args = parser.parse_args()
    SHOW_WIRE = args.show_wire
    try:
        if args.command == "status":
            show_status()
        elif args.command == "race":
            race(args.unsafe, args.count)
        elif args.command == "stats":
            show_stats(args.last)
        elif args.command == "crash":
            ask(args.port, {"msg_type": "coord_crash"}, timeout=3)
            print(f"\nindex-{args.port} stopped with no warning.")
        elif args.command == "crash-leader":
            leaders = {s.get("leader") for s in statuses().values() if s}
            leader = max(p for p in leaders if p)
            ask(leader, {"msg_type": "coord_crash"}, timeout=3)
            print(f"\nLeader index-{leader} stopped with no warning.")
        elif args.command == "events":
            show_events(args.last, args.grep, args.since)
    except (OSError, ValueError, ProtocolError) as error:
        print(f"\nError: {error}\nAre the index nodes running?  python3 index_node/index_node.py 8001")
        sys.exit(1)


if __name__ == "__main__":
    main()
