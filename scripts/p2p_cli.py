"""
Command line tool for the peer-to-peer overlay (Milestone 3).

Every command talks to one peer (the entry point). Any peer in the ring works.

    python3 scripts/p2p_cli.py ring 9101                  show the whole ring, peer by peer
    python3 scripts/p2p_cli.py status 9101                one peer: id, neighbours, finger table, keys
    python3 scripts/p2p_cli.py publish 9101               store every document in the DHT
    python3 scripts/p2p_cli.py lookup 9101 distributed    route a lookup for the key of a word
    python3 scripts/p2p_cli.py search 9101 "distributed systems"
    python3 scripts/p2p_cli.py get-doc 9101 3             find document 3 through the overlay
    python3 scripts/p2p_cli.py leave 9103                 peer 9103 leaves gracefully

Add --show-wire to see a message before and after marshalling.
"""
import argparse
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.logger import get_logger
from common.protocol import ProtocolError, make_message, marshal, request_reply, unmarshal

HOST = "127.0.0.1"
DOCS_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "index_node", "data", "documents.json")
log = get_logger("p2p-cli")
# status calls (used by "ring" many times) are logged to the file only, so the console stays readable
_file_log = logging.getLogger("p2p-cli-status")
if not _file_log.handlers:
    _file_log.setLevel(logging.INFO)
    _file_log.propagate = False
    _handler = logging.FileHandler(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                                "logs", "p2p-cli.log"))
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
    _file_log.addHandler(_handler)
SHOW_WIRE = False


def ask(port, msg_type, timeout=15, **fields):
    message = make_message(msg_type, **fields)
    quiet = _file_log if msg_type == "status" else log
    quiet.info("SENT %s to peer on port %d", msg_type, port)
    if SHOW_WIRE:
        wire = marshal(message)
        print("\n--- MARSHALLING DEMONSTRATION ---")
        print("1. Before marshalling (Python dictionary):", message)
        print("2. After marshalling (UTF-8 bytes):", wire)
        print("3. Unmarshalled by the receiver (Python dictionary):", unmarshal(wire))
    reply = request_reply(HOST, port, message, timeout)
    if SHOW_WIRE:
        print("4. Reply received as bytes:", marshal(reply))
        print("--- END MARSHALLING DEMONSTRATION ---\n")
    if reply.get("msg_type") == "error":
        raise ProtocolError(reply.get("error", "peer returned an error"))
    quiet.info("RECEIVED %s from peer on port %d", reply.get("msg_type"), port)
    return reply


def ref(r):
    return "none" if r is None else f"{r['port']} (id {r['id']})"


def path_text(path, owner=None):
    parts = [f"{r['port']} (id {r['id']})" for r in path]
    if owner is not None and (not path or path[-1]["id"] != owner["id"]):
        parts.append(f"{owner['port']} (id {owner['id']})")
    return " -> ".join(parts)


def show_status(port):
    s = ask(port, "status")
    me = s["peer"]
    print(f"\nPeer {me['port']}  (ring id {me['id']})")
    print(f"  predecessor : {ref(s['predecessor'])}")
    print(f"  successor   : {ref(s['successor'])}")
    print(f"  successor list : {', '.join(ref(r) for r in s['successor_list'])}")
    print(f"  keys stored : {s['keys_total']} ({s['keys_owned']} owned, the rest are replicas)")
    print("  finger table (finger[i] = succ(id + 2^i)):")
    for f in s["fingers"]:
        print(f"    i={f['i']}  start={f['start']:>3}  ->  {ref(f['node'])}")


def show_ring(port):
    start = ask(port, "status")["peer"]
    members, seen, current = [], set(), start["port"]
    for _ in range(64):
        s = ask(current, "status")
        if s["peer"]["id"] in seen:
            break
        seen.add(s["peer"]["id"])
        members.append(s)
        current = s["successor"]["port"]
    print("\nRing (clockwise, following successor pointers):\n")
    print(f"  {'port':<6} {'id':<4} {'predecessor':<16} {'successor':<16} {'keys':<5} owned")
    for s in members:
        me = s["peer"]
        print(f"  {me['port']:<6} {me['id']:<4} {ref(s['predecessor']):<16} {ref(s['successor']):<16} "
              f"{s['keys_total']:<5} {s['keys_owned']}")
    ids = sorted(s["peer"]["id"] for s in members)
    print(f"\n  {len(members)} peers, ids in ring order: {ids}")
    closed = members[-1]["successor"]["id"] == members[0]["peer"]["id"]
    print(f"  ring closes back to the first peer: {closed}")


def main():
    global SHOW_WIRE
    parser = argparse.ArgumentParser(description="Peer-to-peer overlay command line tool")
    parser.add_argument("--show-wire", action="store_true", help="print each message before and after marshalling")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("ring", "status", "publish", "leave"):
        sub.add_parser(name).add_argument("port", type=int)
    p = sub.add_parser("lookup"); p.add_argument("port", type=int); p.add_argument("word")
    p = sub.add_parser("search"); p.add_argument("port", type=int); p.add_argument("query")
    p = sub.add_parser("get-doc"); p.add_argument("port", type=int); p.add_argument("doc_id", type=int)
    args = parser.parse_args()
    SHOW_WIRE = args.show_wire

    try:
        if args.command == "ring":
            show_ring(args.port)
        elif args.command == "status":
            show_status(args.port)
        elif args.command == "publish":
            docs = json.load(open(DOCS_FILE, encoding="utf-8"))
            r = ask(args.port, "publish", docs=docs)
            print(f"\nPublished {r['documents']} documents as {r['keys']} keys into the DHT.")
            print("Keys stored per peer:", ", ".join(f"{p}: {n}" for p, n in r["placed"].items()))
        elif args.command == "lookup":
            key = "term:" + args.word.lower()
            r = ask(args.port, "lookup", key=key)
            print(f"\nLookup of '{key}' (key id {r['key_id']}) entered the ring at peer {args.port}.")
            print("  path :", path_text(r["path"], r["owner"]))
            print(f"  hops : {len(r['path'])}")
            print(f"  owner: peer {ref(r['owner'])}  <- the peer responsible for this key")
        elif args.command == "search":
            r = ask(args.port, "search", query=args.query)
            print(f"\nSearch '{args.query}' through the overlay (entry peer {args.port}):")
            for l in r["lookups"]:
                state = "found" if l["found"] else "no such term"
                print(f"  '{l['word']}' key id {l['key_id']}: {path_text(l['path'], l['owner'])}   ({state})")
            if not r["results"]:
                print("\nNo matching documents found.")
            else:
                print("\nSearch Results\n--------------")
                for n, d in enumerate(r["results"], 1):
                    print(f"{n}. {d['title']}\n   Document ID: {d['doc_id']}\n   Score: {d['score']}")
        elif args.command == "get-doc":
            r = ask(args.port, "get_doc", doc_id=args.doc_id)
            print(f"\nDocument {args.doc_id} (key id {r['key_id']}) discovered at peer {ref(r['owner'])}")
            print("  path :", path_text(r["path"], r["owner"]))
            if r["doc"]:
                print(f"  title: {r['doc']['title']}\n  body : {r['doc']['body']}")
            else:
                print("  not found (not published yet?)")
        elif args.command == "leave":
            r = ask(args.port, "leave")
            print(f"\nPeer {ref(r['peer'])} handed its keys to its successor and left the ring.")
    except (OSError, ProtocolError) as error:
        log.error("%s", error)
        print(f"\nError: {error}\nIs the peer on port {args.port} running?  python3 p2p/peer.py {args.port}")
        sys.exit(1)


if __name__ == "__main__":
    main()
