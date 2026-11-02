"""
P2P bridge (runs inside the gateway process)
CECS 327 - Milestone 3

Lets the normal client use the peer-to-peer overlay through the gateway.
The gateway picks any running peer as the entry point (every peer can route a
lookup), forwards the request, and returns the answer.

Client -> gateway:
    {"request_id": "..", "msg_type": "p2p_search", "query": "distributed systems"}
    {"request_id": "..", "msg_type": "p2p_ring"}

The gateway answers with p2p_search_result or p2p_ring_result (or an error).
"""
import logging

from common.protocol import make_message, request_reply

HOST = "127.0.0.1"
PEER_PORTS = list(range(9101, 9111))      # ports a peer may be running on
TIMEOUT = 8.0

log = logging.getLogger("p2p_bridge")


class P2PUnavailable(Exception):
    pass


def _entry_peer():
    """First peer from the list that answers a ping."""
    for port in PEER_PORTS:
        try:
            request_reply(HOST, port, make_message("ping"), 1.0)
            return port
        except (OSError, ValueError):
            continue
    raise P2PUnavailable("no peer in the P2P overlay is running")


def _error(request, text, kind):
    return {"request_id": request.get("request_id"), "error": text, "error_type": kind}


def _ring(entry):
    """Follow successor pointers from the entry peer, once around the ring."""
    members, seen, port = [], set(), entry
    for _ in range(64):
        s = request_reply(HOST, port, make_message("status"), TIMEOUT)
        if s["peer"]["id"] in seen:
            break
        seen.add(s["peer"]["id"])
        members.append({"port": s["peer"]["port"], "id": s["peer"]["id"],
                        "predecessor": s["predecessor"]["port"] if s["predecessor"] else None,
                        "successor": s["successor"]["port"],
                        "keys_total": s["keys_total"], "keys_owned": s["keys_owned"]})
        port = s["successor"]["port"]
    return members


def handle(request):
    kind = request.get("msg_type")
    try:
        entry = _entry_peer()
        log.info("P2P %s: entry peer is %d", kind, entry)
        if kind == "p2p_search":
            query = request.get("query")
            if not isinstance(query, str) or not query.strip():
                return _error(request, "query must be a non-empty string", "invalid_request")
            reply = request_reply(HOST, entry, make_message("search", query=query.strip()), TIMEOUT)
            if reply.get("msg_type") == "error":
                return _error(request, reply.get("error", "peer error"), "p2p_error")
            return {"request_id": request.get("request_id"), "msg_type": "p2p_search_result",
                    "entry_peer": entry, "results": reply["results"], "lookups": reply["lookups"]}
        return {"request_id": request.get("request_id"), "msg_type": "p2p_ring_result",
                "entry_peer": entry, "members": _ring(entry)}
    except P2PUnavailable as error:
        log.error("P2P unavailable: %s", error)
        return _error(request, str(error) + " (start peers with scripts/run_p2p.sh)", "p2p_unavailable")
    except (OSError, ValueError, KeyError) as error:
        log.error("P2P error: %s", error)
        return _error(request, f"peer-to-peer request failed: {error}", "p2p_error")
