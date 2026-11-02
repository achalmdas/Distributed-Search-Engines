"""
Guided demo of the peer-to-peer overlay (Milestone 3).

Starts real peers, then walks through: peers starting, publishing documents,
a lookup routed through the ring, resource discovery, a peer joining, a peer
leaving, and a peer crashing. Each step prints the command it runs, so you
can repeat any step by hand. Log lines from the peers are shown after the
steps that matter.

    python3 scripts/p2p_demo.py            run straight through
    python3 scripts/p2p_demo.py --pause    wait for Enter between steps (for recording)

Uses ports 9101-9105. Nothing else may be running on them.
"""
import argparse
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
CLI = os.path.join("scripts", "p2p_cli.py")
procs = {}
PAUSE = False


def step(title):
    print("\n" + "=" * 72 + f"\n{title}\n" + "=" * 72)
    if PAUSE:
        input("(press Enter to run this step) ")


def run(*args):
    shown = "python3 " + " ".join(args)
    print(f"\n$ {shown}")
    subprocess.run([PY, *args], cwd=ROOT)


def start(port, join=None):
    args = [PY, os.path.join("p2p", "peer.py"), str(port)] + (["--join", str(join)] if join else [])
    print(f"$ python3 {' '.join(args[1:])}" + ("" if join else "      (first peer: creates the ring)"))
    procs[port] = subprocess.Popen(args, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    time.sleep(0.8)


def show_log(port, words, last=6):
    path = os.path.join(ROOT, "logs", f"peer-{port}.log")
    try:
        lines = [l.rstrip() for l in open(path, encoding="utf-8") if any(w in l for w in words)]
    except OSError:
        return
    print(f"\n--- logs/peer-{port}.log (last {min(last, len(lines))} matching lines) ---")
    for line in lines[-last:]:
        print(line)


def main():
    global PAUSE
    parser = argparse.ArgumentParser()
    parser.add_argument("--pause", action="store_true", help="wait for Enter between steps")
    PAUSE = parser.parse_args().pause
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
    try:
        step("STEP 1: four peers start and form a ring (each new peer joins through 9101)")
        start(9101)
        for port in (9102, 9103, 9104):
            start(port, join=9101)
        time.sleep(3)
        run(CLI, "ring", "9101")
        show_log(9103, ["PEER STARTED", "JOIN", "FINGER TABLE"])

        step("STEP 2: publish the documents; each word and document goes to the peer that owns its key")
        run(CLI, "publish", "9101")

        step("STEP 3: a lookup request routed through the overlay (watch the hop path)")
        run(CLI, "lookup", "9103", "systems")
        show_log(9103, ["LOOKUP", "FORWARD"], 3)

        step("STEP 4: resource discovery: search and fetch a document through the peers")
        run(CLI, "search", "9104", "distributed systems")
        run(CLI, "get-doc", "9102", "3")

        step("STEP 5: a peer joins (9105 joins through 9101 and takes over part of the key space)")
        start(9105, join=9101)
        time.sleep(3)
        run(CLI, "ring", "9101")
        show_log(9105, ["JOIN", "PREDECESSOR", "SUCCESSOR"])
        run(CLI, "search", "9105", "distributed systems")

        step("STEP 6: a peer leaves gracefully (9102 hands its keys to its successor)")
        run(CLI, "leave", "9102")
        time.sleep(3)
        run(CLI, "ring", "9101")
        run(CLI, "search", "9104", "distributed systems")

        step("STEP 7: a peer crashes (9103 is killed with no goodbye); the ring repairs itself")
        time.sleep(3)
        print("\n$ kill peer 9103")
        procs[9103].kill()
        time.sleep(5)
        run(CLI, "ring", "9101")
        run(CLI, "search", "9104", "distributed systems")
        show_log(9101, ["PEER LOST", "PREDECESSOR", "SUCCESSOR"])
        show_log(9104, ["PEER LOST", "PREDECESSOR", "SUCCESSOR"])
        print("\nDone. Full logs are in the logs/ folder (peer-<port>.log).")
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.kill()


if __name__ == "__main__":
    main()
