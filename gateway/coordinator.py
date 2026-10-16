"""
Group coordinator (runs inside the gateway process)
CECS 327 - Milestone 2

Handles "update" and "status" requests from clients:

  1. assign the next update_id (one coordinator = one total order)
  2. multicast the update to every member of the group: the 3 index shards
     (each stores only its own documents) and the 3 replicas (each stores
     all documents). Members ack; a member that is behind is sent the
     updates it missed, in order (reliable, totally ordered multicast)
  3. publish the same update as an event on topic "index.updates" (pub-sub)
  4. compare digests: replicas must match each other, and each shard must
     match the replicas' digest for that shard

Client -> gateway:
    {"request_id": "..", "msg_type": "update", "doc": {"doc_id": 16, "title": "..", "body": ".."}}
    {"request_id": "..", "msg_type": "status"}
"""
import logging
import threading

from common.protocol import make_message, request_reply

HOST = "127.0.0.1"
SHARD_PORTS = [8001, 8002, 8003]
REPLICA_PORTS = [8011, 8012, 8013]
MEMBERS = SHARD_PORTS + REPLICA_PORTS
BROKER = ("127.0.0.1", 9000)
TOPIC = "index.updates"
TIMEOUT = 2.0

log = logging.getLogger("coordinator")

_lock = threading.Lock()          # one update at a time = total order
_update_log = []                  # _update_log[i] is update_id i+1
_next = {p: 1 for p in MEMBERS}   # next update_id each member still needs


def _name(port):
    return f"index-{port}" if port in SHARD_PORTS else f"replica-{port}"


def _send_to_member(port, upto, result):
    """Bring one member up to update `upto`, sending missed updates in order."""
    reply, tries = None, 0
    while _next[port] <= upto and tries < 2 * upto + 2:
        tries += 1
        uid = _next[port]
        try:
            log.info("GROUP SEND update_id=%d to %s", uid, _name(port))
            reply = request_reply(HOST, port, _update_log[uid - 1], TIMEOUT)
        except (OSError, ValueError, ConnectionError) as error:
            log.error("GROUP %s unreachable: %s", _name(port), error)
            result[port] = {"status": "failed", "error": str(error)}
            return
        if reply.get("msg_type") not in ("ack", "nack") or "next_update_id" not in reply:
            result[port] = {"status": "failed", "error": str(reply)}
            return
        log.info("GROUP %s from %s: update_id=%d status=%s digest=%s next_update_id=%d",
                 reply["msg_type"].upper(), _name(port), uid, reply.get("status", ""),
                 reply.get("digest"), reply["next_update_id"])
        # The member tells us what it needs next. If that is not uid+1 it is
        # behind (restarted or missed an update), so we replay from there.
        if reply["next_update_id"] <= uid:
            log.warning("GROUP %s is behind, replaying from update_id=%d", _name(port), reply["next_update_id"])
        _next[port] = reply["next_update_id"]
    if reply is not None and _next[port] > upto:
        result[port] = {"status": "ack", "digest": reply.get("digest"), "doc_count": reply.get("doc_count"),
                        "shard_digests": reply.get("shard_digests")}
    elif reply is None:
        result[port] = {"status": "ack"}        # nothing was needed
    else:
        result[port] = {"status": "failed", "error": "could not catch up"}


def _publish(update_id, msg):
    try:
        reply = request_reply(*BROKER, make_message("publish", topic=TOPIC, event_id=update_id, payload=msg), TIMEOUT)
        return reply.get("delivered", 0)
    except (OSError, ValueError, ConnectionError) as error:
        log.warning("PUBLISH failed (broker down?): %s", error)
        return 0


def _check_sync(result):
    """True only if every member acked and all digests agree."""
    if any(result.get(p, {}).get("status") != "ack" for p in MEMBERS):
        return False
    rep = [result[p] for p in REPLICA_PORTS]
    if len({r["digest"] for r in rep}) != 1:
        return False
    for k, port in enumerate(SHARD_PORTS):
        if any(r["shard_digests"][str(k)] != result[port]["digest"] for r in rep):
            return False
    return True


def handle_update(request):
    doc = request.get("doc")
    if (not isinstance(doc, dict) or not isinstance(doc.get("doc_id"), int)
            or not isinstance(doc.get("title"), str) or not isinstance(doc.get("body"), str)):
        return {"request_id": request.get("request_id"), "error": "doc needs int doc_id, str title, str body",
                "error_type": "invalid_request"}
    with _lock:
        update_id = len(_update_log) + 1
        msg = make_message("index_update", update_id=update_id, doc=doc)
        _update_log.append(msg)
        log.info("UPDATE update_id=%d doc_id=%d title=%r -> multicast to %d members",
                 update_id, doc["doc_id"], doc["title"], len(MEMBERS))

        result, threads = {}, []
        for port in MEMBERS:
            t = threading.Thread(target=_send_to_member, args=(port, update_id, result))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        delivered = _publish(update_id, msg)
        in_sync = _check_sync(result)
        acks = sum(1 for r in result.values() if r["status"] == "ack")
        log.info("GROUP RESULT update_id=%d acks=%d/%d in_sync=%s event_delivered_to=%d",
                 update_id, acks, len(MEMBERS), in_sync, delivered)

    def slim(port):
        r = dict(result[port]); r.pop("shard_digests", None); return r
    return {"request_id": request.get("request_id"), "msg_type": "update_result", "update_id": update_id,
            "owner_shard": SHARD_PORTS[doc["doc_id"] % len(SHARD_PORTS)],
            "shards": {str(p): slim(p) for p in SHARD_PORTS},
            "replicas": {str(p): slim(p) for p in REPLICA_PORTS},
            "event_delivered_to": delivered, "in_sync": in_sync}


def handle_status(request):
    """Ask every member for its current state (changes nothing)."""
    result = {}
    for port in MEMBERS:
        try:
            r = request_reply(HOST, port, make_message("status"), TIMEOUT)
            result[port] = {"status": "ack", "doc_count": r.get("doc_count"), "digest": r.get("digest"),
                            "shard_digests": r.get("shard_digests"), "next_update_id": r.get("next_update_id"),
                            "events_received": r.get("events_received")}
        except (OSError, ValueError, ConnectionError) as error:
            result[port] = {"status": "down", "error": str(error)}
    with _lock:
        expected = len(_update_log) + 1
    caught_up = all(r.get("next_update_id") == expected for r in result.values() if r["status"] == "ack")
    in_sync = _check_sync(result) and caught_up
    def slim(port):
        r = dict(result[port]); r.pop("shard_digests", None); return r
    return {"request_id": request.get("request_id"), "msg_type": "status_result",
            "shards": {str(p): slim(p) for p in SHARD_PORTS},
            "replicas": {str(p): slim(p) for p in REPLICA_PORTS}, "in_sync": in_sync}


def handle(request):
    if request.get("msg_type") == "update":
        return handle_update(request)
    return handle_status(request)