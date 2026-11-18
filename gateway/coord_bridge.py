"""
Coordination status (runs inside the gateway process)
CECS 327 - Milestone 4

The index nodes 8001-8003 elect a leader (Bully) and run a centralized lock
(see index_node/coordination.py). This module lets the client see that state
through the gateway:

    client -> gateway : {"request_id": "..", "msg_type": "coord_status", "sender_id": "client-..", "lamport_ts": 3}
    gateway -> client : {"request_id": "..", "msg_type": "coord_status_result",
                         "leader": {"id": "index-8003", "port": 8003, "term": 1},
                         "mutex": {"resource": "shared/search_stats.json", "owner": null, "queue_length": 0},
                         "nodes": [{"id": "index-8001", "port": 8001, "up": true, "leader": 8003,
                                    "lamport_ts": 41}, ...]}

The gateway adds its own "sender_id" and "lamport_ts" to the reply (gateway.py).
"""
import logging

from common.protocol import request_reply

HOST = "127.0.0.1"
NODES = [8001, 8002, 8003]
TIMEOUT = 1.5

log = logging.getLogger("coord_bridge")


def name(port):
    return f"index-{port}"


def handle_status(request, clock):
    """Ask every index node for its view; report the leader that the live nodes agree on.

    Advances the gateway Lamport clock on each send (tick) and each reply (receive),
    matching query_index_node(), so the final client reply stamp is causally after
    every node timestamp embedded in this response.
    """
    nodes, views = [], {}
    for port in NODES:
        try:
            r = request_reply(HOST, port, {"msg_type": "coord_status", "sender_id": "gateway-8000",
                                           "lamport_ts": clock.tick()}, TIMEOUT)
        except (OSError, ValueError) as error:
            nodes.append({"id": name(port), "port": port, "up": False, "error": str(error)})
            continue
        clock.receive(r.get("lamport_ts"))
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
    log.info("COORD STATUS leader=%s agreed=%s holder=%s queue=%s", leader_port, agreed, holder,
             info.get("lock_queue"))
    return {
        "request_id": request.get("request_id"),
        "msg_type": "coord_status_result",
        "leader": {"id": name(leader_port), "port": leader_port, "term": info.get("term")} if agreed else {},
        "agreed": agreed,
        "mutex": {"resource": info.get("resource", "shared/search_stats.json"),
                  "owner": name(holder) if holder else None,
                  "queue": [name(p) for p in info.get("lock_queue", [])],
                  "queue_length": len(info.get("lock_queue", []))},
        "nodes": nodes,
    }
