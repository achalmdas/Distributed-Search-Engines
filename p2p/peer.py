"""
Chord-style peer node (Milestone 3: P2P integration).
CECS 327

Every peer runs this same program. Peers form a logical ring (an overlay on top
of TCP) and together they work as a distributed hash table (DHT) for the search
index:

    identifier space   : 0 .. 2^M - 1  (M = 8, so 256 positions on the ring)
    peer id            : sha1("127.0.0.1:<port>") mod 2^M
    key id             : sha1(key text) mod 2^M
    ownership rule     : owner(k) = succ(k), the first live peer clockwise from k
    finger table       : finger[i] = succ(n + 2^i), so each hop roughly halves
                         the distance and a lookup takes O(log N) hops
    successor list     : the next R peers on the ring, used to survive crashes
    replication        : every key lives on its owner and on the owner's successor

What is stored (the resources that peers discover):

    "term:<word>"  ->  postings  {doc_id: {"title": ..., "count": n}}
    "doc:<id>"     ->  the full document {doc_id, title, body}

Messages are the same newline-terminated JSON objects used in Milestone 2
(common/protocol.py). Message types between peers:

    find_successor   route a key id through the ring (recursive, carries the hop path)
    get_predecessor  / notify / ping     ring maintenance (stabilization)
    join_request     a new peer asks its successor to take it in (and get its keys)
    update_successor / update_predecessor / handoff     leave and repair
    put / get / replicate                store, read and copy keys

Messages from a client (any peer can be the entry point): lookup, search,
get_doc, publish, status, leave.

Run from the repository root:

    python3 p2p/peer.py 9101                    # first peer: creates the ring
    python3 p2p/peer.py 9102 --join 9101        # later peers join through any peer
"""

import argparse
import hashlib
import os
import re
import socket
import sys
import threading
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.logger import get_logger
from common.protocol import ProtocolError, make_message, recv_msg, request_reply, send_msg

HOST = "127.0.0.1"
M = 8                          # identifier bits
RING = 2 ** M                  # identifier space size
R = 3                          # successor list length
MAX_HOPS = 16                  # a lookup that goes longer than this is broken
RPC_TIMEOUT = 2.0              # peer to peer call
FORWARD_TIMEOUT = 6.0          # a forwarded lookup waits for the rest of the path
STABILIZE_EVERY = 1.0          # seconds between maintenance rounds
QUIET = {"get_predecessor", "notify", "ping", "replicate", "status"}   # periodic traffic, not logged


class OverlayError(Exception):
    """A peer answered with an error, or no route could be found."""


# ------------------------------------------------------------ ring arithmetic
def hash_id(text: str) -> int:
    """Map any text to a position on the ring."""
    return int(hashlib.sha1(text.encode("utf-8")).hexdigest(), 16) % RING


def peer_id_for(port: int) -> int:
    return hash_id(f"{HOST}:{port}")


def in_range(x: int, a: int, b: int) -> bool:
    """x in the half-open ring interval (a, b]. If a == b it is the whole ring."""
    if a < b:
        return a < x <= b
    if a > b:
        return x > a or x <= b
    return True


def between_open(x: int, a: int, b: int) -> bool:
    """x in the open ring interval (a, b). If a == b it is everything except a."""
    if a < b:
        return a < x < b
    if a > b:
        return x > a or x < b
    return x != a


def tokenize(text: str):
    return re.findall(r"\w+", text.lower())


def fmt(ref) -> str:
    return "none" if ref is None else f"{ref['port']}(id {ref['id']})"


def fmt_path(path, owner=None) -> str:
    """Hop path as text; if the owner is given and not already last, it is added at the end."""
    refs = list(path)
    if owner is not None and (not refs or refs[-1]["id"] != owner["id"]):
        refs.append(owner)
    return " -> ".join(f"{r['port']}(id {r['id']})" for r in refs)


# ------------------------------------------------------------ the peer
class Peer:
    def __init__(self, port: int, node_id: int = None):
        self.port = port
        self.id = peer_id_for(port) if node_id is None else node_id % RING
        self.log = get_logger(f"peer-{port}")
        self.lock = threading.RLock()
        self.successor_list = [self.ref()]          # [0] is the successor; alone = ourselves
        self.predecessor = None
        self.fingers = [None] * M
        self.store = {}                             # key text -> value
        self._replica_state = None
        self._tick = 0
        self.leaving = False                        # set once a graceful leave starts

    def ref(self):
        return {"id": self.id, "port": self.port}

    # ---------------------------------------------------------- messaging
    def rpc(self, target, msg_type, quiet=False, timeout=RPC_TIMEOUT, **fields):
        message = make_message(msg_type, sender=self.ref(), **fields)
        if not quiet:
            self.log.info("SENT %s to peer %s", msg_type, fmt(target))
        reply = request_reply(HOST, target["port"], message, timeout)
        if reply.get("msg_type") == "error":
            raise OverlayError(reply.get("error", "remote error"))
        return reply

    # ---------------------------------------------------------- routing state
    def successor(self):
        with self.lock:
            return self.successor_list[0]

    def forget(self, ref):
        """A peer stopped answering: drop it from every routing structure."""
        with self.lock:
            before = len(self.successor_list)
            self.successor_list = [r for r in self.successor_list if r["id"] != ref["id"]]
            if not self.successor_list:
                self.successor_list = [self.ref()]
            self.fingers = [None if (f and f["id"] == ref["id"]) else f for f in self.fingers]
            if self.predecessor and self.predecessor["id"] == ref["id"]:
                self.predecessor = None
            changed = before != len(self.successor_list)
        self.log.warning("PEER LOST: %s is not answering, removed from routing state (successor is now %s)",
                         fmt(ref), fmt(self.successor()))
        return changed

    def candidates(self, key_id):
        """Peers to forward to, best first: closest preceding fingers, then the successor list."""
        with self.lock:
            pool = [f for f in self.fingers if f] + list(self.successor_list)
        seen, preceding, rest = set(), [], []
        for c in pool:
            if c["id"] == self.id or c["id"] in seen:
                continue
            seen.add(c["id"])
            (preceding if between_open(c["id"], self.id, key_id) else rest).append(c)
        preceding.sort(key=lambda c: (c["id"] - self.id) % RING, reverse=True)   # farthest jump first
        return preceding + rest

    def route(self, key_id, hops, quiet=False):
        """Chord lookup. Returns (owner ref, hop path). Forwards recursively through the overlay.
        quiet=True is used by background maintenance so it does not flood the logs."""
        hops = hops + [self.ref()]
        if len(hops) > MAX_HOPS:
            raise OverlayError("lookup exceeded the hop limit")
        with self.lock:
            pred = self.predecessor
            succ = self.successor_list[0]
        if pred is not None and in_range(key_id, pred["id"], self.id):
            return self.ref(), hops                       # key is in our own range
        if in_range(key_id, self.id, succ["id"]):
            return succ, hops                             # our successor owns it
        last_error = None
        for cand in self.candidates(key_id):
            try:
                if not quiet:
                    self.log.info("FORWARD lookup key_id=%d to peer %s (path so far: %s)",
                                  key_id, fmt(cand), fmt_path(hops))
                reply = self.rpc(cand, "find_successor", quiet=quiet, timeout=FORWARD_TIMEOUT,
                                 key_id=key_id, hops=hops, maintenance=quiet)
                return reply["owner"], reply["hops"]
            except (OSError, ProtocolError):
                self.forget(cand)
            except (OverlayError, KeyError) as error:
                last_error = error
        raise OverlayError(f"no route to key {key_id}: {last_error or 'no reachable peer'}")

    def fix_fingers(self):
        """finger[i] = succ(n + 2^i)."""
        new = []
        for i in range(M):
            try:
                owner, _ = self.route((self.id + 2 ** i) % RING, [], quiet=True)
            except OverlayError:
                owner = None
            new.append(owner)
        with self.lock:
            changed = new != self.fingers
            self.fingers = new
        if changed:
            self.log.info("FINGER TABLE updated: %s", self.finger_summary())

    def finger_summary(self):
        with self.lock:
            return ", ".join(f"+{2 ** i}->{f['id'] if f else '?'}" for i, f in enumerate(self.fingers))

    # ---------------------------------------------------------- storing keys
    def owned_items(self):
        with self.lock:
            if self.predecessor is None:
                return {}
            pid = self.predecessor["id"]
            return {k: v for k, v in self.store.items() if in_range(hash_id(k), pid, self.id)}

    def store_local(self, key, value, merge):
        with self.lock:
            if merge and isinstance(value, dict) and isinstance(self.store.get(key), dict):
                self.store[key].update(value)
            else:
                self.store[key] = value
            return self.store[key]

    def put_key(self, key, value, merge=False, bulk=False):
        """Find the owner of key through the overlay and store the value there."""
        owner, hops = self.lookup_key(key, bulk)
        if owner["id"] == self.id:
            self.local_put(key, value, merge, bulk)
        else:
            self.rpc(owner, "put", quiet=bulk, key=key, value=value, merge=merge, bulk=bulk)
        return owner

    def local_put(self, key, value, merge, bulk):
        stored = self.store_local(key, value, merge)
        if not bulk:
            self.log.info("STORED key=%s (key_id %d) as owner", key, hash_id(key))
        succ = self.successor()
        if succ["id"] != self.id:                         # copy to the successor
            try:
                self.rpc(succ, "replicate", quiet=True, items={key: stored})
            except (OSError, ProtocolError, OverlayError):
                pass

    def lookup_key(self, key, quiet=False):
        key_id = hash_id(key)
        owner, hops = self.route(key_id, [])
        if not quiet:
            self.log.info("LOOKUP key=%s key_id=%d -> owner %s, path: %s",
                          key, key_id, fmt(owner), fmt_path(hops, owner))
        return owner, hops

    def fetch(self, key):
        """Look up key and read it from the owner. Returns (value or None, owner, hops)."""
        last = None
        for _ in range(3):
            owner, hops = self.lookup_key(key)
            if owner["id"] == self.id:
                with self.lock:
                    return self.store.get(key), owner, hops
            try:
                reply = self.rpc(owner, "get", key=key)
                return (reply["value"] if reply.get("found") else None), owner, hops
            except (OSError, ProtocolError) as error:
                last = error
                self.forget(owner)
                time.sleep(0.3)
        raise OverlayError(f"could not read {key}: {last}")

    # ---------------------------------------------------------- joining and leaving
    def create_ring(self):
        self.log.info("CREATED a new ring: this peer (id %d) is the only member", self.id)

    def join(self, bootstrap_port):
        self.log.info("JOINING through peer on port %d", bootstrap_port)
        boot = self.rpc({"id": -1, "port": bootstrap_port}, "ping", quiet=True)["peer"]
        reply = self.rpc(boot, "find_successor", key_id=self.id, hops=[])
        owner = reply["owner"]
        if owner["id"] == self.id:
            raise OverlayError(f"id {self.id} is already taken by peer {fmt(owner)}; use another port")
        self.log.info("JOIN: my id %d belongs just before %s (lookup path: %s)",
                      self.id, fmt(owner), fmt_path(reply["hops"], owner))
        jr = self.rpc(owner, "join_request", node=self.ref())
        pred = jr["predecessor"]
        with self.lock:
            self.successor_list = [owner]
            self.predecessor = pred
            self.store.update(jr["keys"])
        if pred["id"] != owner["id"]:
            try:
                self.rpc(pred, "update_successor", node=self.ref())
            except (OSError, ProtocolError, OverlayError):
                pass
        self.fix_fingers()
        self.log.info("JOINED the ring: predecessor %s, successor %s, received %d keys from the successor",
                      fmt(pred), fmt(owner), len(jr["keys"]))

    def leave(self):
        """Graceful leave: hand our keys to the successor and unlink from the ring."""
        with self.lock:
            self.leaving = True                     # stops maintenance, so we never re-announce ourselves
            succ = self.successor_list[0]
            pred = self.predecessor
            keys = dict(self.store)
        self.log.info("LEAVING the ring: handing %d keys to successor %s", len(keys), fmt(succ))
        if succ["id"] != self.id:
            try:
                self.rpc(succ, "handoff", leaving=self.ref(), predecessor=pred, keys=keys)
            except (OSError, ProtocolError, OverlayError) as error:
                self.log.error("handoff to %s failed: %s", fmt(succ), error)
            if pred is not None and pred["id"] != self.id:
                try:
                    self.rpc(pred, "update_successor", node=succ, leaving=self.ref())
                except (OSError, ProtocolError, OverlayError) as error:
                    self.log.error("telling predecessor failed: %s", error)
        self.log.info("LEFT the ring")

    # ---------------------------------------------------------- maintenance (stabilization)
    def stabilize(self):
        reply = None
        while reply is None:
            succ = self.successor()
            if succ["id"] == self.id:
                with self.lock:
                    pred = self.predecessor
                if pred is not None and pred["id"] != self.id:
                    with self.lock:
                        self.successor_list = [pred]
                    self.log.info("STABILIZE: ring of two formed, successor is now %s", fmt(pred))
                    continue
                return
            try:
                reply = self.rpc(succ, "get_predecessor", quiet=True)
            except (OSError, ProtocolError, OverlayError):
                self.forget(succ)
        succ = self.successor()
        x = reply.get("predecessor")
        if x and x["id"] != self.id and between_open(x["id"], self.id, succ["id"]):
            try:
                self.rpc(x, "ping", quiet=True)
                self.log.info("STABILIZE: peer %s sits between us and %s, new successor", fmt(x), fmt(succ))
                old, succ = succ, x
            except (OSError, ProtocolError, OverlayError):
                old = None
        else:
            old = None
        new, seen = [succ], {succ["id"]}
        for r in ([old] if old else []) + reply.get("successor_list", []):
            if r["id"] != self.id and r["id"] not in seen and len(new) < R:
                new.append(r)
                seen.add(r["id"])
        with self.lock:
            self.successor_list = new
        try:
            self.rpc(succ, "notify", quiet=True, node=self.ref())
        except (OSError, ProtocolError, OverlayError):
            pass

    def check_predecessor(self):
        with self.lock:
            pred = self.predecessor
        if pred is None or pred["id"] == self.id:
            return
        try:
            self.rpc(pred, "ping", quiet=True)
        except (OSError, ProtocolError, OverlayError):
            with self.lock:
                if self.predecessor and self.predecessor["id"] == pred["id"]:
                    self.predecessor = None
            self.log.warning("PEER LOST: predecessor %s is not answering", fmt(pred))

    def replicate_owned(self, force=False):
        """Keep a copy of every key we own on our successor (replication factor 2)."""
        with self.lock:
            pred, succ = self.predecessor, self.successor_list[0]
        if pred is None or succ["id"] == self.id:
            return
        items = self.owned_items()
        state = (pred["id"], succ["id"], len(items))
        if state == self._replica_state and not force:
            return
        try:
            self.rpc(succ, "replicate", quiet=True, items=items)
            self._replica_state = state
            self.log.info("REPLICATED %d owned keys to successor %s", len(items), fmt(succ))
        except (OSError, ProtocolError, OverlayError):
            pass

    def maintenance_loop(self):
        while True:
            time.sleep(STABILIZE_EVERY)
            if self.leaving:
                return
            try:
                self._tick += 1
                self.stabilize()
                self.check_predecessor()
                if self._tick % 3 == 0:
                    self.fix_fingers()
                self.replicate_owned(force=(self._tick % 10 == 0))
            except Exception as error:                       # never let maintenance die
                self.log.error("maintenance error: %s", error)

    # ---------------------------------------------------------- message handlers
    def on_ping(self, msg):
        return make_message("pong", peer=self.ref())

    def on_find_successor(self, msg):
        key_id = int(msg["key_id"])
        owner, hops = self.route(key_id, msg.get("hops", []), quiet=bool(msg.get("maintenance")))
        return make_message("successor_found", owner=owner, hops=hops)

    def on_get_predecessor(self, msg):
        with self.lock:
            return make_message("predecessor", predecessor=self.predecessor,
                                successor_list=list(self.successor_list))

    def on_notify(self, msg):
        node = msg["node"]
        with self.lock:
            pred = self.predecessor
            if node["id"] != self.id and (pred is None or between_open(node["id"], pred["id"], self.id)):
                self.predecessor = node
                self.log.info("PREDECESSOR is now %s", fmt(node))
            if self.successor_list[0]["id"] == self.id and node["id"] != self.id:
                self.successor_list = [node]
        return make_message("notify_ok")

    def on_join_request(self, msg):
        node = msg["node"]
        if node["id"] == self.id:
            raise OverlayError(f"id {node['id']} is already used by this peer")
        with self.lock:
            pred = self.predecessor or self.ref()
            keys = {k: v for k, v in self.store.items() if in_range(hash_id(k), pred["id"], node["id"])}
            self.predecessor = node
            if self.successor_list[0]["id"] == self.id:
                self.successor_list = [node]
        self.log.info("JOIN accepted: peer %s is my new predecessor, sending it %d keys", fmt(node), len(keys))
        return make_message("join_reply", predecessor=pred, keys=keys)

    def on_update_successor(self, msg):
        node, leaving = msg["node"], msg.get("leaving")
        if leaving:
            self.forget_quietly(leaving)
        with self.lock:
            if node["id"] == self.id:
                self.successor_list = [self.ref()]
            else:
                self.successor_list = [node] + [r for r in self.successor_list
                                                if r["id"] not in (node["id"], self.id)][:R - 1]
        self.log.info("SUCCESSOR is now %s", fmt(self.successor()))
        return make_message("ok")

    def forget_quietly(self, ref):
        with self.lock:
            self.successor_list = [r for r in self.successor_list if r["id"] != ref["id"]] or [self.ref()]
            self.fingers = [None if (f and f["id"] == ref["id"]) else f for f in self.fingers]
            if self.predecessor and self.predecessor["id"] == ref["id"]:
                self.predecessor = None

    def on_handoff(self, msg):
        leaving, pred, keys = msg["leaving"], msg.get("predecessor"), msg.get("keys", {})
        with self.lock:
            self.store.update(keys)
        self.forget_quietly(leaving)
        with self.lock:
            self.predecessor = pred if (pred and pred["id"] not in (self.id, leaving["id"])) else None
        self.log.info("HANDOFF: peer %s left, took over %d keys, predecessor is now %s",
                      fmt(leaving), len(keys), fmt(self.predecessor))
        return make_message("ok", stored=len(keys))

    def on_put(self, msg):
        self.local_put(msg["key"], msg["value"], bool(msg.get("merge")), bool(msg.get("bulk")))
        return make_message("put_ok", key=msg["key"])

    def on_get(self, msg):
        with self.lock:
            found = msg["key"] in self.store
            value = self.store.get(msg["key"])
        self.log.info("GET key=%s -> %s", msg["key"], "found" if found else "not found")
        return make_message("get_reply", found=found, value=value)

    def on_replicate(self, msg):
        with self.lock:
            self.store.update(msg.get("items", {}))
        return make_message("ok")

    def on_lookup(self, msg):
        key = msg["key"]
        owner, hops = self.lookup_key(key)
        return make_message("lookup_result", key=key, key_id=hash_id(key), owner=owner, path=hops)

    def on_search(self, msg):
        words = list(dict.fromkeys(tokenize(msg.get("query", ""))))
        if not words:
            raise OverlayError("query has no words")
        scores, lookups = {}, []
        for word in words:
            postings, owner, hops = self.fetch("term:" + word)
            lookups.append({"word": word, "key_id": hash_id("term:" + word), "owner": owner,
                            "path": hops, "found": postings is not None})
            for doc_id, info in (postings or {}).items():
                entry = scores.setdefault(int(doc_id), {"doc_id": int(doc_id), "title": info["title"], "score": 0})
                entry["score"] += info["count"]
        results = sorted(scores.values(), key=lambda r: (-r["score"], r["doc_id"]))[:10]
        self.log.info("SEARCH %r -> %d results", msg.get("query"), len(results))
        return make_message("search_result", query=msg.get("query"), results=results, lookups=lookups)

    def on_get_doc(self, msg):
        key = f"doc:{int(msg['doc_id'])}"
        doc, owner, hops = self.fetch(key)
        return make_message("doc_result", doc=doc, owner=owner, path=hops, key_id=hash_id(key))

    def on_publish(self, msg):
        docs = msg.get("docs", [])
        postings = {}
        placed = Counter()
        for doc in docs:
            owner = self.put_key(f"doc:{doc['doc_id']}", doc, bulk=True)
            placed[owner["port"]] += 1
            counts = Counter(tokenize(doc["title"] + " " + doc["body"]))
            for word, count in counts.items():
                postings.setdefault(word, {})[str(doc["doc_id"])] = {"title": doc["title"], "count": count}
        for word, post in postings.items():
            owner = self.put_key("term:" + word, post, merge=True, bulk=True)
            placed[owner["port"]] += 1
        self.log.info("PUBLISHED %d documents as %d keys; keys stored per peer: %s",
                      len(docs), len(docs) + len(postings), dict(sorted(placed.items())))
        return make_message("publish_result", documents=len(docs), keys=len(docs) + len(postings),
                            placed={str(p): n for p, n in sorted(placed.items())})

    def on_status(self, msg):
        owned = self.owned_items()
        with self.lock:
            fingers = [{"i": i, "start": (self.id + 2 ** i) % RING, "node": f}
                       for i, f in enumerate(self.fingers)]
            return make_message("status_reply", peer=self.ref(), predecessor=self.predecessor,
                                successor=self.successor_list[0], successor_list=list(self.successor_list),
                                fingers=fingers, keys_total=len(self.store), keys_owned=len(owned))

    def on_leave(self, msg):
        self.leave()
        threading.Timer(0.4, lambda: os._exit(0)).start()      # exit after the reply is sent
        return make_message("left", peer=self.ref())

    # ---------------------------------------------------------- server
    HANDLERS = {"ping", "find_successor", "get_predecessor", "notify", "join_request", "update_successor",
                "handoff", "put", "get", "replicate", "lookup", "search", "get_doc", "publish", "status", "leave"}

    def handle(self, conn, addr):
        kind = "?"
        try:
            msg = recv_msg(conn)
            kind = msg.get("msg_type", "?")
            if kind not in self.HANDLERS:
                raise OverlayError(f"unknown msg_type: {kind}")
            if self.leaving and kind not in ("leave", "status"):
                raise OverlayError("this peer is leaving the ring")
            noisy = kind in QUIET or msg.get("maintenance") or (kind == "put" and msg.get("bulk"))
            if not noisy:
                sender = msg.get("sender")
                self.log.info("RECEIVED %s from %s", kind, fmt(sender) if sender else "client")
            reply = getattr(self, "on_" + kind)(msg)
            send_msg(conn, reply)
            if not noisy:
                self.log.info("SENT %s reply", reply.get("msg_type"))
        except (OverlayError, ProtocolError, KeyError, ValueError, TypeError) as error:
            self.log.error("error handling %s: %s", kind, error)
            try:
                send_msg(conn, make_message("error", error=str(error)))
            except OSError:
                pass
        except (ConnectionError, OSError):
            pass
        finally:
            conn.close()

    def serve(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, self.port))
        server.listen(64)
        self.log.info("PEER STARTED: port %d, id %d (ring of %d positions)", self.port, self.id, RING)
        threading.Thread(target=self.accept_loop, args=(server,), daemon=True).start()

    def accept_loop(self, server):
        while True:
            conn, addr = server.accept()
            threading.Thread(target=self.handle, args=(conn, addr), daemon=True).start()


def main():
    parser = argparse.ArgumentParser(description="Chord-style peer (Milestone 3)")
    parser.add_argument("port", type=int, help="port this peer listens on")
    parser.add_argument("--join", type=int, metavar="PORT", help="port of any peer already in the ring")
    parser.add_argument("--id", type=int, help="use this ring position instead of hashing the port")
    args = parser.parse_args()

    peer = Peer(args.port, args.id)
    peer.serve()
    try:
        if args.join:
            peer.join(args.join)
        else:
            peer.create_ring()
    except (OSError, ProtocolError, OverlayError) as error:
        peer.log.error("could not join the ring: %s", error)
        sys.exit(1)
    threading.Thread(target=peer.maintenance_loop, daemon=True).start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        peer.log.info("peer stopped")


if __name__ == "__main__":
    main()
