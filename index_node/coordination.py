"""
Coordination between the index nodes (runs inside every index node process)
CECS 327 - Milestone 4

Adds three things to the index nodes 8001, 8002 and 8003:

  1. Leader election - Bully algorithm. The live node with the highest port
     is the leader. A node that notices the leader is gone (heartbeat fails)
     sends ELECTION to every higher node. If none answers, it becomes leader
     and sends COORDINATOR to everyone. If a higher node answers OK, that node
     takes over the election and we wait for its COORDINATOR message.

  2. Distributed mutual exclusion - centralized algorithm. The leader is the
     lock coordinator: lock_request is granted now or queued (FIFO), and on
     lock_release the leader grants the lock to the next node in the queue.
     A new leader asks every node whether it holds or is waiting for the
     lock, so the lock survives a leader failure.

  3. The shared resource - shared/search_stats.json. Every search goes to all
     three shards at once, and each shard records it in this one file (read,
     modify, write). Without the lock two shards read the same old total and
     one write is lost; with the lock every write is kept.

Every message between nodes carries a Lamport timestamp ("lamport_ts", see
lamport.py) and every coordination event is logged as "[L=<n>] ...", so the
logs of all nodes can be merged into one ordered history.

Message formats (one JSON object per line, newline-terminated):
  election     node -> higher node : {"msg_type": "election", "sender_id": "index-8001", "lamport_ts": 12}
               higher node -> node : {"msg_type": "election_ok", "node_id": "index-8002", "lamport_ts": 14}
  coordinator  leader -> every node : {"msg_type": "coordinator", "sender_id": "index-8003", "lamport_ts": 20, "term": 1}
  heartbeat    node -> leader       : {"msg_type": "heartbeat", "sender_id": "index-8001", "lamport_ts": 30}
  lock_request node -> leader       : {"msg_type": "lock_request", "sender_id": "index-8001", "lamport_ts": 41}
               leader -> node       : {"msg_type": "lock_granted"} or {"msg_type": "lock_queued", "position": 1}
  lock_release node -> leader       : {"msg_type": "lock_release", "sender_id": "index-8001", "lamport_ts": 47}
  lock_grant   leader -> next node  : {"msg_type": "lock_grant", "sender_id": "index-8003", "lamport_ts": 50}
  lock_state   new leader -> node   : {"msg_type": "lock_state"}
               node -> new leader   : {"msg_type": "lock_state_reply", "holding": false, "waiting": true}
  coord_status anyone -> node       : {"msg_type": "coord_status"}
               node -> anyone       : {"msg_type": "coord_status_reply", "leader": 8003, "lamport_ts": 61, ...}
  stats_write  anyone -> node       : {"msg_type": "stats_write", "count": 10, "use_lock": true}
               node -> anyone       : {"msg_type": "stats_write_result", "writes": 10}
  coord_crash  demo -> node         : {"msg_type": "coord_crash"}   (the node stops with no warning)
"""
import json
import os
import threading
import time
from collections import deque

import lamport
from common.protocol import ProtocolError, request_reply

# ---------------------------------------------------------------- config
HOST = "127.0.0.1"
PEERS = [8001, 8002, 8003]          # every index node; a higher port wins the election
RPC_TIMEOUT = 1.0
HEARTBEAT_EVERY = 1.0
COORDINATOR_WAIT = 3.0              # how long to wait for a higher node to announce itself
LOCK_TIMEOUT = 20.0                 # give up waiting for the lock after this long
WORK_DELAY = 0.05                   # time spent inside the critical section (makes races visible)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_FILE = os.environ.get("COORD_SHARED_FILE", os.path.join(ROOT, "shared", "search_stats.json"))

NETWORK_ERRORS = (OSError, ConnectionError, ProtocolError, ValueError)

# ---------------------------------------------------------------- shared state
PORT = None                          # set by start()
log = None

state_lock = threading.Lock()        # protects everything below
leader = None
term = 0                             # election term: goes up by one with every new leader
electing = False
coordinator_seen = threading.Event()

lock_holder = None                   # lock coordinator state, only used while we are the leader
lock_queue = deque()

local_mutex = threading.Lock()       # one thread of this node at a time asks for the lock
holding = False                      # our own lock state as a requester
waiting = False
granted = threading.Event()


def name(port):
    return f"index-{port}"


def sender_port(msg):
    """"index-8001" -> 8001"""
    return int(msg["sender_id"].rsplit("-", 1)[1])


def event(text, *args, at=None):
    """Log a local event with its Lamport time (ticks the clock unless `at` is given)."""
    stamp = lamport.tick() if at is None else at
    log.info("[L=%d] " + text, stamp, *args)
    return stamp


def send(port, msg_type, timeout=RPC_TIMEOUT, quiet=False, **fields):
    """Send one message to another index node with our Lamport time, return its reply."""
    stamp = lamport.tick()
    message = {"msg_type": msg_type, "sender_id": name(PORT), "lamport_ts": stamp, **fields}
    if not quiet:
        log.info("[L=%d] SEND %s to %s", stamp, msg_type, name(port))
    reply = request_reply(HOST, port, message, timeout)
    stamp = lamport.receive(reply.get("lamport_ts"))
    if not quiet:
        log.info("[L=%d] REPLY %s from %s", stamp, reply.get("msg_type"), name(port))
    return reply


def others():
    return [p for p in PEERS if p != PORT]


# ---------------------------------------------------------------- startup and heartbeat
def start(port, logger):
    global PORT, log
    PORT, log = port, logger
    event("COORDINATION STARTED on %s (peers %s)", name(PORT), PEERS)
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    start_election("node started")


def heartbeat_loop():
    global leader
    while True:
        time.sleep(HEARTBEAT_EVERY)
        with state_lock:
            current, busy, holder = leader, electing, lock_holder
        if busy:
            continue
        if current is None:
            start_election("no leader known")
        elif current != PORT:
            try:
                send(current, "heartbeat", quiet=True)
            except NETWORK_ERRORS:
                event("LEADER %s is not answering heartbeats", name(current))
                with state_lock:
                    if leader == current:
                        leader = None
                start_election(f"leader {name(current)} failed")
        elif holder not in (None, PORT):
            try:                                              # leader: is the lock holder still alive?
                send(holder, "heartbeat", quiet=True)
            except NETWORK_ERRORS:
                event("LOCK HOLDER %s crashed, taking the lock back", name(holder))
                release_for(holder)


# ---------------------------------------------------------------- leader election (Bully)
def start_election(reason):
    global electing
    with state_lock:
        if electing:
            return
        electing = True
    threading.Thread(target=run_election, args=(reason,), daemon=True).start()


def run_election(reason):
    global electing
    try:
        while True:
            event("ELECTION started (%s)", reason)
            coordinator_seen.clear()
            answered = []
            for port in [p for p in PEERS if p > PORT]:
                try:
                    if send(port, "election").get("msg_type") == "election_ok":
                        answered.append(port)
                except NETWORK_ERRORS:
                    event("ELECTION: %s did not answer", name(port))
            if not answered:
                become_leader()
                return
            event("ELECTION: higher node(s) %s answered OK, waiting for COORDINATOR",
                  [name(p) for p in answered])
            if coordinator_seen.wait(COORDINATOR_WAIT):
                return
            reason = "no COORDINATOR message arrived"
    finally:
        with state_lock:
            electing = False


def become_leader():
    global leader, lock_holder, lock_queue, term
    event("ELECTION won: no higher node answered, collecting the lock state")
    holder, queue, seen_term = None, [], 0
    for port in others():                                     # rebuild the lock after a failover
        try:
            reply = send(port, "lock_state")
        except NETWORK_ERRORS:
            continue
        seen_term = max(seen_term, int(reply.get("term", 0)))
        if reply.get("holding"):
            holder = port
        elif reply.get("waiting"):
            queue.append(port)
    with state_lock:
        if holding:
            holder = PORT
        elif waiting:
            queue.append(PORT)
        leader, lock_holder, lock_queue = PORT, holder, deque(queue)
        term = max(term, seen_term) + 1                       # newer than any term a live node has seen
    event("I AM THE LEADER (%s is the highest live node), term %d", name(PORT), term)
    event("LOCK STATE recovered: holder=%s queue=%s",
          name(holder) if holder else "none", [name(p) for p in queue])
    for port in others():
        try:
            send(port, "coordinator", term=term)
        except NETWORK_ERRORS:
            event("COORDINATOR: %s is down", name(port))
    coordinator_seen.set()
    if holder is None:
        grant_next()


def on_election(msg):
    sender = sender_port(msg)
    event("ELECTION message from %s, answering OK and taking over", name(sender))
    start_election(f"{name(sender)} started an election")
    return {"msg_type": "election_ok"}


def on_coordinator(msg):
    global leader, lock_holder, lock_queue, term
    sender = sender_port(msg)
    with state_lock:
        leader = sender
        term = max(term, int(msg.get("term", 0)))
        if sender != PORT:
            lock_holder, lock_queue = None, deque()           # only the leader keeps lock state
    coordinator_seen.set()
    event("LEADER is now %s", name(sender))
    if sender < PORT:                                         # Bully: a lower node cannot lead while we are alive
        start_election(f"lower node {name(sender)} claimed leadership")
    return {"msg_type": "ok"}


# ---------------------------------------------------------------- mutual exclusion: leader side
def on_lock_request(msg):
    global lock_holder
    node = sender_port(msg)
    with state_lock:
        if leader != PORT:
            return {"msg_type": "error", "error": "not the leader", "leader": leader}
        if lock_holder in (None, node):
            lock_holder = node
            is_granted = True
        else:
            if node not in lock_queue:
                lock_queue.append(node)
            is_granted, holder, queue = False, lock_holder, list(lock_queue)
    if is_granted:
        event("LOCK GRANTED to %s", name(node))
        return {"msg_type": "lock_granted"}
    event("LOCK BUSY: held by %s, %s queued (queue=%s)",
          name(holder), name(node), [name(p) for p in queue])
    return {"msg_type": "lock_queued", "position": queue.index(node) + 1}


def on_lock_release(msg):
    node = sender_port(msg)
    event("LOCK RELEASED by %s", name(node))
    release_for(node)
    return {"msg_type": "ok"}


def release_for(node):
    global lock_holder
    with state_lock:
        if lock_holder != node:
            return
        lock_holder = None
    grant_next()


def grant_next():
    global lock_holder
    with state_lock:
        if lock_holder is not None or not lock_queue:
            return
        node = lock_queue.popleft()
        lock_holder = node
    event("LOCK GRANTED to %s (next in queue)", name(node))
    if node == PORT:
        granted.set()
        return
    threading.Thread(target=push_grant, args=(node,), daemon=True).start()


def push_grant(node):
    try:
        send(node, "lock_grant")
    except NETWORK_ERRORS:
        event("LOCK GRANT to %s failed, it is down", name(node))
        release_for(node)


def on_lock_state(msg):
    with state_lock:
        return {"msg_type": "lock_state_reply", "holding": holding, "waiting": waiting, "term": term}


# ---------------------------------------------------------------- mutual exclusion: requester side
def on_lock_grant(msg):
    granted.set()
    return {"msg_type": "ok"}


def to_leader(msg_type):
    """Send a lock message to the leader (or handle it here if we are the leader)."""
    with state_lock:
        current = leader
    if current is None:
        raise ConnectionError("no leader")
    if current == PORT:
        handler = on_lock_request if msg_type == "lock_request" else on_lock_release
        return current, handler({"sender_id": name(PORT)})
    return current, send(current, msg_type)


def acquire():
    """Block until this node holds the distributed lock. Returns False on timeout."""
    global holding, waiting
    local_mutex.acquire()
    granted.clear()
    with state_lock:
        waiting = True
    event("REQUEST lock for %s", os.path.relpath(SHARED_FILE, ROOT))
    deadline = time.time() + LOCK_TIMEOUT
    asked = None                                              # leader we are queued at
    while time.time() < deadline:
        with state_lock:
            current = leader
        if granted.is_set():
            break
        if current is None or current != asked:               # new leader: ask again
            try:
                asked, reply = to_leader("lock_request")
                if reply.get("msg_type") == "lock_granted":
                    break
                if reply.get("msg_type") != "lock_queued":
                    asked = None
            except NETWORK_ERRORS:
                asked = None
        granted.wait(0.2)
    else:
        with state_lock:
            waiting = False
        event("LOCK TIMEOUT: gave up after %.0fs", LOCK_TIMEOUT)
        local_mutex.release()
        return False
    with state_lock:
        waiting, holding = False, True
    event("ENTER critical section")
    return True


def release():
    global holding
    with state_lock:
        holding = False
    event("EXIT critical section")
    for _ in range(int(LOCK_TIMEOUT / 0.2)):
        try:
            to_leader("lock_release")
            break
        except NETWORK_ERRORS:                                # leader down: the new leader learns
            time.sleep(0.2)                                   # from lock_state that we let go
    local_mutex.release()


# ---------------------------------------------------------------- the shared resource
def read_stats():
    try:
        with open(SHARED_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {"total": 0, "per_node": {}, "history": []}


def write_file(data):
    os.makedirs(os.path.dirname(SHARED_FILE), exist_ok=True)
    tmp = f"{SHARED_FILE}.{os.getpid()}.{threading.get_ident()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, SHARED_FILE)                              # readers never see a half-written file


def write_stats(query, use_lock=True):
    """Record one search in the shared file. Read-modify-write, so it needs the lock."""
    if use_lock and not acquire():
        return False
    try:
        data = read_stats()
        time.sleep(WORK_DELAY)                                # work done while holding the old value
        stamp = lamport.tick()
        data["total"] += 1
        data["per_node"][name(PORT)] = data["per_node"].get(name(PORT), 0) + 1
        data["history"].append({"node": name(PORT), "lamport_ts": stamp, "query": query,
                                "total_after": data["total"]})
        data["history"] = data["history"][-200:]
        write_file(data)
        event("WROTE %s: total=%d query=%r%s", os.path.relpath(SHARED_FILE, ROOT),
              data["total"], query, "" if use_lock else " (NO LOCK)", at=stamp)
        return True
    finally:
        if use_lock:
            release()


def record_search(query):
    threading.Thread(target=write_stats, args=(query,), daemon=True).start()


def on_stats_write(msg):
    count, use_lock = int(msg.get("count", 10)), bool(msg.get("use_lock", True))
    event("STATS DEMO: %d writes %s the lock", count, "with" if use_lock else "WITHOUT")
    done = sum(1 for i in range(count) if write_stats(f"demo-{PORT}-{i}", use_lock))
    return {"msg_type": "stats_write_result", "writes": done}


def on_coord_status(msg):
    with state_lock:
        return {"msg_type": "coord_status_reply", "leader": leader, "term": term, "electing": electing,
                "resource": os.path.relpath(SHARED_FILE, ROOT),
                "holding": holding, "waiting": waiting,
                "lock_holder": lock_holder, "lock_queue": list(lock_queue)}


def on_heartbeat(msg):
    return {"msg_type": "heartbeat_ok"}


def on_coord_crash(msg):
    """Demo and test only: stop this node with no warning, like a crash."""
    event("CRASH requested: stopping now without telling anyone")
    threading.Timer(0.2, lambda: os._exit(1)).start()
    return {"msg_type": "crashing"}


# ---------------------------------------------------------------- request handling
HANDLERS = {
    "election": on_election,
    "coordinator": on_coordinator,
    "heartbeat": on_heartbeat,
    "lock_request": on_lock_request,
    "lock_release": on_lock_release,
    "lock_grant": on_lock_grant,
    "lock_state": on_lock_state,
    "coord_status": on_coord_status,
    "stats_write": on_stats_write,
    "coord_crash": on_coord_crash,
}


def handle(msg):
    reply = HANDLERS[msg["msg_type"]](msg)
    reply["node_id"] = name(PORT)
    return reply
