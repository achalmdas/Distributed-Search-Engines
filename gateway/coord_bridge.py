"""
Coordination status and distributed lock client (runs inside the gateway process)
CECS 327 - Milestone 4

The index nodes 8001-8003 elect a leader (Bully) and run a centralized lock
(see index_node/coordination.py). This module lets the client see that state
through the gateway, and lets the gateway acquire the same lock around document
updates:

    client -> gateway : {"request_id": "..", "msg_type": "coord_status", "sender_id": "client-..", "lamport_ts": 3}
    gateway -> client : {"request_id": "..", "msg_type": "coord_status_result",
                         "leader": {"id": "index-8003", "port": 8003, "term": 1},
                         "mutex": {"resource": "shared/search_stats.json; index updates", "owner": null,
                                   "queue_length": 0},
                         "nodes": [{"id": "index-8001", "port": 8001, "up": true, "leader": 8003,
                                    "lamport_ts": 41}, ...]}

    gateway -> leader : {"msg_type": "lock_acquire", "sender_id": "gateway-8000", "lamport_ts": N}
    gateway -> leader : {"msg_type": "lock_release", "sender_id": "gateway-8000", "lamport_ts": N}

The gateway adds its own "sender_id" and "lamport_ts" to the reply (gateway.py).
"""
import logging
import time
import uuid

from common.protocol import ProtocolError, request_reply

HOST = "127.0.0.1"
NODES = [8001, 8002, 8003]
GATEWAY_ID = "gateway-8000"
TIMEOUT = 1.5
LOCK_TIMEOUT = 20.0
LOCK_RPC_TIMEOUT = LOCK_TIMEOUT + 2.0

log = logging.getLogger("coord_bridge")

NETWORK_ERRORS = (OSError, ValueError, ConnectionError, ProtocolError)


def name(port):
    return f"index-{port}"


def new_lock_client_id():
    """Unique id per concurrent gateway update so two updates cannot share one lock slot."""
    return f"{GATEWAY_ID}-{uuid.uuid4().hex[:8]}"


def _rpc(port, msg_type, clock, sender_id=GATEWAY_ID, timeout=TIMEOUT, **fields):
    reply = request_reply(HOST, port, {
        "msg_type": msg_type,
        "sender_id": sender_id,
        "lamport_ts": clock.tick(),
        **fields,
    }, timeout)
    clock.receive(reply.get("lamport_ts"))
    return reply


def find_leader(clock):
    """Return the agreed leader port, or None if nodes disagree / are down."""
    leaders = set()
    for port in NODES:
        try:
            reply = _rpc(port, "coord_status", clock)
        except NETWORK_ERRORS:
            continue
        leader = reply.get("leader")
        if isinstance(leader, int):
            leaders.add(leader)
    if len(leaders) == 1:
        return leaders.pop()
    return None


def acquire_lock(clock):
    """Block until the elected leader grants the distributed lock.

    Returns the lock client id on success, or None on timeout. Pass that id to
    release_lock so concurrent gateway updates stay distinct queue entries.
    """
    holder = new_lock_client_id()
    deadline = time.time() + LOCK_TIMEOUT
    while time.time() < deadline:
        leader = find_leader(clock)
        if leader is None:
            time.sleep(0.2)
            continue
        try:
            log.info("REQUEST distributed lock from %s as %s", name(leader), holder)
            reply = _rpc(leader, "lock_acquire", clock, sender_id=holder, timeout=LOCK_RPC_TIMEOUT)
        except NETWORK_ERRORS as error:
            log.warning("lock_acquire to %s failed: %s", name(leader), error)
            time.sleep(0.2)
            continue
        if reply.get("msg_type") == "lock_granted":
            log.info("ENTER critical section (distributed lock held by %s)", holder)
            return holder
        log.warning("lock_acquire reply from %s: %s", name(leader), reply)
        time.sleep(0.2)
    log.error("DISTRIBUTED LOCK TIMEOUT after %.0fs", LOCK_TIMEOUT)
    return None


def release_lock(clock, holder):
    """Release the distributed lock held under `holder` at the current leader."""
    if not holder:
        return False
    log.info("EXIT critical section (releasing distributed lock %s)", holder)
    deadline = time.time() + LOCK_TIMEOUT
    while time.time() < deadline:
        leader = find_leader(clock)
        if leader is None:
            time.sleep(0.2)
            continue
        try:
            reply = _rpc(leader, "lock_release", clock, sender_id=holder)
            if reply.get("msg_type") == "ok":
                log.info("RELEASED distributed lock %s via %s", holder, name(leader))
                return True
            log.warning("lock_release reply from %s: %s", name(leader), reply)
        except NETWORK_ERRORS as error:
            log.warning("lock_release to %s failed: %s", name(leader), error)
        time.sleep(0.2)
    log.error("Could not release distributed lock %s before timeout", holder)
    return False


def handle_status(request, clock):
    """Ask every index node for its view; report the leader that the live nodes agree on.

    Advances the gateway Lamport clock on each send (tick) and each reply (receive),
    matching query_index_node(), so the final client reply stamp is causally after
    every node timestamp embedded in this response.
    """
    nodes, views = [], {}
    for port in NODES:
        try:
            r = _rpc(port, "coord_status", clock)
        except NETWORK_ERRORS as error:
            nodes.append({"id": name(port), "port": port, "up": False, "error": str(error)})
            continue
        views[port] = r
        nodes.append({"id": name(port), "port": port, "up": True, "leader": r.get("leader"),
                      "term": r.get("term"), "lamport_ts": r.get("lamport_ts"),
                      "holding": r.get("holding"), "waiting": r.get("waiting")})
    if not views:
        return {"request_id": request.get("request_id"), "error": "no index node is running",
                "error_type": "coord_unavailable"}

    leaders = {v.get("leader") for v in views.values()}
    agreed = len(leaders) == 1 and None not in leaders
    leader_port = leaders.pop() if agreed else None
    info = views.get(leader_port, {})                   # the leader holds the lock state
    holder = info.get("lock_holder")
    queue = list(info.get("lock_queue", []))
    log.info("COORD STATUS leader=%s agreed=%s holder=%s queue=%s", leader_port, agreed, holder, queue)
    return {
        "request_id": request.get("request_id"),
        "msg_type": "coord_status_result",
        "leader": {"id": name(leader_port), "port": leader_port, "term": info.get("term")} if agreed else {},
        "agreed": agreed,
        "mutex": {"resource": info.get("resource", "shared/search_stats.json; index updates"),
                  "owner": holder,
                  "queue": queue,
                  "queue_length": len(queue)},
        "nodes": nodes,
    }
