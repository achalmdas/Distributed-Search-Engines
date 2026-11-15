"""
Guided demo of Milestone 4 (coordination between the index nodes), good for the video.

Starts the 3 index nodes and the gateway and walks through: the Bully election as
the nodes start, logical timestamps in messages, mutual exclusion on the shared
file shared/search_stats.json, the same race without the lock (lost updates), the
leader failing and being replaced, the old leader coming back, the leader failing
while nodes wait for the lock, and the log lines of all nodes in Lamport order.

    python3 scripts/coord_demo.py            run straight through
    python3 scripts/coord_demo.py --pause    wait for Enter between steps (for recording)

Uses ports 8000-8003. Stop run_all first.
"""
import argparse
import glob
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
CLI = os.path.join("scripts", "coord_cli.py")
procs = {}
PAUSE = False


def step(title):
    print("\n" + "=" * 78 + f"\n{title}\n" + "=" * 78)
    if PAUSE:
        input("(press Enter to run this step) ")


def run(*args):
    print(f"\n$ python3 {' '.join(args)}")
    subprocess.run([PY, *args], cwd=ROOT)


def start(port):
    print(f"$ python3 index_node/index_node.py {port}")
    procs[port] = subprocess.Popen([PY, os.path.join("index_node", "index_node.py"), str(port)], cwd=ROOT,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


def show_log(port, words, last=8):
    path = os.path.join(ROOT, "logs", f"index-{port}.log")
    try:
        lines = [l.rstrip() for l in open(path, encoding="utf-8") if any(w in l for w in words)]
    except OSError:
        return
    print(f"\n--- logs/index-{port}.log (last {min(last, len(lines))} matching lines) ---")
    for line in lines[-last:]:
        print(line[11:].replace(" INFO: ", " "))


def now():
    return time.strftime("%H:%M:%S")


def main():
    global PAUSE
    parser = argparse.ArgumentParser()
    parser.add_argument("--pause", action="store_true", help="wait for Enter between steps")
    PAUSE = parser.parse_args().pause
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
    for path in glob.glob(os.path.join(ROOT, "logs", "index-80*.log")):
        os.remove(path)                                   # fresh logs for this run
    stats = os.path.join(ROOT, "shared", "search_stats.json")
    if os.path.exists(stats):
        os.remove(stats)                                  # start the shared file at 0
    try:
        step("STEP 1: the 3 index nodes start; each new higher node wins the Bully election")
        for port in (8001, 8002, 8003):
            start(port)
            time.sleep(1.0)
        print("$ python3 gateway/gateway.py")
        procs[8000] = subprocess.Popen([PY, os.path.join("gateway", "gateway.py")], cwd=ROOT,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        time.sleep(3)
        run(CLI, "status")
        show_log(8001, ["ELECTION", "LEADER"], 8)

        step("STEP 2: logical timestamps travel in every message")
        run("client/client.py", "--coord-status", "--show-wire")
        show_log(8001, ["SEND", "REPLY", "RECEIVED from"], 6)

        step("STEP 3: mutual exclusion: a search makes all 3 shards update the shared file at once")
        mark = now()
        run("client/client.py", "--search", "distributed systems")
        time.sleep(1)
        show_log(8003, ["LOCK GRANTED", "LOCK BUSY", "LOCK RELEASED"], 8)
        run(CLI, "stats", "--last", "3")

        step("STEP 4: the 3 nodes race for the shared file 10 times each, WITH the lock")
        run(CLI, "race")

        step("STEP 5: the same race WITHOUT the lock: the conflict the lock prevents")
        run(CLI, "race", "--unsafe")

        step("STEP 6: the leader (index-8003) fails; the others elect index-8002")
        mark = now()
        run(CLI, "crash", "8003")
        time.sleep(4)
        run(CLI, "status")
        run(CLI, "events", "--since", mark, "--grep", "not answering|ELECTION|LEADER|LOCK STATE")

        step("STEP 7: index-8003 comes back and takes over again (it has the highest port)")
        start(8003)
        time.sleep(4)
        run(CLI, "status")

        step("STEP 8: the leader fails WHILE the other nodes wait for the lock")
        print("\n$ python3 scripts/coord_cli.py race --count 20   (in the background)")
        racer = subprocess.Popen([PY, CLI, "race", "--count", "20"], cwd=ROOT,
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        time.sleep(0.6)
        mark = now()
        run(CLI, "crash", "8003")
        out, _ = racer.communicate(timeout=90)
        print(out)
        run(CLI, "status")
        run(CLI, "events", "--since", mark, "--grep", "ELECTION won|I AM THE LEADER|LOCK STATE")

        step("STEP 9: event ordering: the lock events of all nodes merged in Lamport order")
        run(CLI, "events", "--grep", "LOCK GRANTED|LOCK RELEASED|LOCK STATE", "--last", "14")
        print("\nDone. Full logs: logs/index-8001.log, index-8002.log, index-8003.log")
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.kill()


if __name__ == "__main__":
    main()
