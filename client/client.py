"""
Client (User / Request Side)
CECS 327 - Distributed Search Engine, Milestones 1-4

Run from the repository root:
    python3 client/client.py
    python3 client/client.py --search "distributed systems"
    python3 client/client.py --update 16 "Raft Consensus" "leader election and log replication"
    python3 client/client.py --status
    python3 client/client.py --p2p-search "distributed systems"
    python3 client/client.py --p2p-ring
    python3 client/client.py --search "distributed systems" --show-wire
    python3 client/client.py --coord-status --show-wire
    python3 client/client.py --demo-conflict 16 --evidence-file logs/m4_conflict.json
    python3 client/client.py --observe-leader 60 --evidence-file logs/m4_leader.json

The client talks ONLY to the gateway at 127.0.0.1:8000. The gateway
handles index shards, group replication, publication to the broker, and
(Milestone 3) lookups through the peer-to-peer overlay.
All TCP messages use the shared common.protocol format: one UTF-8 JSON
object terminated by a newline.
"""

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
import socket
import subprocess
import sys
import threading
import uuid

# Allow "python3 client/client.py" to import the shared project modules.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common.logger import get_logger
from common.protocol import MAX_MESSAGE_BYTES, ProtocolError, marshal, recv_bytes, unmarshal

HOST = "127.0.0.1"
PORT = 8000
SOCKET_TIMEOUT_SECONDS = 15
log = get_logger("client")
CLIENT_ID = f"client-{os.getpid()}-{uuid.uuid4().hex[:8]}"


class LamportClock:
    """One thread-safe logical clock per client process (not wall-clock time)."""

    def __init__(self, initial=0):
        if type(initial) is not int or initial < 0:
            raise ValueError("initial logical time must be a non-negative integer")
        self._value = initial
        self._lock = threading.Lock()

    def send(self):
        """Increment for a local send event and return the outgoing timestamp."""
        with self._lock:
            self._value += 1
            return self._value

    def receive(self, remote_time=None):
        """Apply max(local, incoming)+1 on a receive event."""
        if remote_time is not None and (type(remote_time) is not int or remote_time < 0):
            raise ProtocolError("response lamport_ts must be a non-negative integer")
        with self._lock:
            self._value = max(self._value, remote_time or 0) + 1
            return self._value

    def read(self):
        with self._lock:
            return self._value


CLOCK = LamportClock()


def receive_message(client_socket):
    """Read one complete TCP frame and unmarshal the JSON response."""
    return unmarshal(recv_bytes(client_socket))


def exchange_message(request, show_wire=False):
    """Marshal, send a request to the gateway, and receive its reply."""
    try:
        # Every client send is a Lamport event. Add metadata without changing
        # the existing query/update/status schema used in Milestones 1-3.
        request = dict(request)
        request["sender_id"] = CLIENT_ID
        request["lamport_ts"] = CLOCK.send()
        wire_request = marshal(request)
        if len(wire_request) > MAX_MESSAGE_BYTES:
            raise ProtocolError("request exceeds the maximum allowed size")

        if show_wire:
            print("\n--- MARSHALLING DEMONSTRATION ---")
            print("1. Before marshalling (Python dictionary):", request)
            print("2. After marshalling (UTF-8 bytes):", wire_request)
            print("3. Unmarshalled request (Python dictionary):", unmarshal(wire_request))

        # One request and one reply per TCP connection, like Milestone 1.
        with socket.create_connection((HOST, PORT), timeout=SOCKET_TIMEOUT_SECONDS) as client_socket:
            client_socket.settimeout(SOCKET_TIMEOUT_SECONDS)
            client_socket.sendall(wire_request)
            log.info("CLOCK SEND local=%d sender=%s request_id=%s",
                     request["lamport_ts"], CLIENT_ID, request["request_id"])
            log.info("SENT: %s", request)

            wire_response = recv_bytes(client_socket)
            response = unmarshal(wire_response)
            # M3 services do not yet echo lamport_ts; a legacy response still
            # advances the local receive clock, without inventing remote time.
            remote_time = response.get("lamport_ts")
            local_time = CLOCK.receive(remote_time)
            log.info("CLOCK RECEIVE remote=%s local=%d sender=%s request_id=%s",
                     remote_time if remote_time is not None else "legacy/absent",
                     local_time, response.get("sender_id", "gateway"), request["request_id"])
            log.info("RECEIVED: %s", response)

        if show_wire:
            print("4. Reply received as bytes:", wire_response)
            print("5. After unmarshalling (Python dictionary):", response)
            print("6. Lamport clock after receive:", CLOCK.read())
            if "lamport_ts" not in response:
                print("   (Gateway has not yet implemented Milestone 4 timestamps.)")
            print("--- END MARSHALLING DEMONSTRATION ---\n")

        # Match the reply to the outgoing request, even across many queries.
        if response.get("request_id") != request["request_id"]:
            raise ProtocolError("gateway returned a mismatched request_id")
        return response

    except ConnectionRefusedError:
        log.error("Could not connect to gateway at %s:%d. Start the gateway first.", HOST, PORT)
    except (socket.timeout, TimeoutError):
        log.error("Gateway response timed out.")
    except (ProtocolError, ConnectionError) as error:
        log.error("Invalid or incomplete message: %s", error)
    except OSError as error:
        log.error("Network error: %s", error)
    return None


def send_request(query, show_wire=False):
    """Milestone 1 search protocol; unchanged for the gateway and shards."""
    if not isinstance(query, str) or not query.strip():
        log.error("Search query cannot be empty.")
        return None
    return exchange_message({"request_id": str(uuid.uuid4()), "query": query.strip()}, show_wire)


def send_update(doc_id, title, body, show_wire=False):
    """Submit a document to the gateway's group coordinator."""
    if type(doc_id) is not int or doc_id < 0:
        log.error("Document ID must be a non-negative integer.")
        return None
    if not isinstance(title, str) or not title.strip():
        log.error("Document title cannot be empty.")
        return None
    if not isinstance(body, str) or not body.strip():
        log.error("Document body cannot be empty.")
        return None

    request = {
        "request_id": str(uuid.uuid4()),
        "msg_type": "update",
        "doc": {"doc_id": doc_id, "title": title.strip(), "body": body.strip()},
    }
    return exchange_message(request, show_wire)


def send_status(show_wire=False):
    """Request the current group replication/synchronization status."""
    request = {"request_id": str(uuid.uuid4()), "msg_type": "status"}
    return exchange_message(request, show_wire)


def send_p2p_search(query, show_wire=False):
    """Milestone 3: search through the peer-to-peer overlay (via the gateway)."""
    if not isinstance(query, str) or not query.strip():
        log.error("Search query cannot be empty.")
        return None
    request = {"request_id": str(uuid.uuid4()), "msg_type": "p2p_search", "query": query.strip()}
    return exchange_message(request, show_wire)


def send_p2p_ring(show_wire=False):
    """Milestone 3: ask the overlay for the list of peers in ring order."""
    request = {"request_id": str(uuid.uuid4()), "msg_type": "p2p_ring"}
    return exchange_message(request, show_wire)


def send_coord_status(show_wire=False):
    """M4: request the elected leader and mutual-exclusion state from gateway.

    The gateway asks the index nodes 8001-8003 (gateway/coord_bridge.py) and
    returns the leader they agree on, its term, and the lock state.
    """
    return exchange_message({"request_id": str(uuid.uuid4()), "msg_type": "coord_status"}, show_wire)


def display_coord_status(response):
    """Display the M4 status contract; do not infer unavailable coordinator state."""
    if response is not None and response.get("error_type") == "invalid_request" \
            and "query" in str(response.get("error", "")):
        print("\nThis gateway does not support coordination status (it is an older version).")
        print("Use the Milestone 4 gateway, which answers 'coord_status'.")
        return
    if display_error(response):
        return
    if response.get("msg_type") != "coord_status_result":
        log.error("Unexpected coordination response: %s", response)
        return
    leader = response.get("leader") or {}
    mutex = response.get("mutex") or {}
    if not isinstance(leader, dict) or not isinstance(mutex, dict):
        log.error("Gateway sent malformed coordinator status: %s", response)
        return
    print("\nCoordination Status")
    print("-------------------")
    print("Current leader:", leader.get("id", "Unknown"))
    print("Leader address/port:", leader.get("port", "Unknown"))
    print("Election epoch:", leader.get("term", "Unknown"))
    print("Shared resource:", mutex.get("resource", "Unknown"))
    print("Current lock holder:", mutex.get("owner", "None"))
    print("Requests queued:", mutex.get("queue_length", "Unknown"))
    print("Gateway Lamport timestamp:", response.get("lamport_ts", "Not supplied"))
    if response.get("nodes") is not None:
        print("Node clocks / membership:")
        print(json.dumps(response["nodes"], indent=2, sort_keys=True))


def utc_now():
    """UTC timestamps for evidence files; NOT a substitute for Lamport time."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def write_evidence(path, record):
    """Write auditable client-observed evidence, without inventing server events."""
    if not path:
        return
    try:
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        with open(path, "w", encoding="utf-8") as output:
            json.dump(record, output, indent=2, sort_keys=True)
            output.write("\n")
        print(f"Evidence written to: {path}")
    except OSError as exc:
        log.error("Could not save evidence at %s: %s", path, exc)
        raise


def parse_child_update(output):
    """Extract the actual structured gateway reply printed by a child process."""
    match = re.search(r"^M4_RESULT_JSON=(.+)$", output, re.MULTILINE)
    if not match:
        return None
    try:
        value = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def verify_group_reply(response):
    """Check one response: distinct network members acknowledged and agreed."""
    if not isinstance(response, dict) or response.get("msg_type") != "update_result":
        return False
    if type(response.get("update_id")) is not int or response["update_id"] < 1:
        return False
    if response.get("in_sync") is not True:
        return False
    shards, replicas = response.get("shards"), response.get("replicas")
    if not isinstance(shards, dict) or len(shards) != 3:
        return False
    if not isinstance(replicas, dict) or len(replicas) != 3:
        return False
    if not all(isinstance(info, dict) and info.get("status") == "ack"
               for info in [*shards.values(), *replicas.values()]):
        return False
    digests = [r.get("digest") for r in replicas.values()]
    return all(isinstance(digest, str) and digest for digest in digests) \
        and len(set(digests)) == 1


def verify_final_status(status, highest_update_id, winning_reply):
    """Confirm every shard/replica is caught up and replicas match the last write."""
    if not isinstance(status, dict) or status.get("msg_type") != "status_result":
        return False
    if status.get("in_sync") is not True:
        return False
    shards, replicas = status.get("shards"), status.get("replicas")
    if not isinstance(shards, dict) or len(shards) != 3:
        return False
    if not isinstance(replicas, dict) or len(replicas) != 3:
        return False
    for info in [*shards.values(), *replicas.values()]:
        if not isinstance(info, dict) or info.get("status") != "ack":
            return False
        next_id = info.get("next_update_id")
        if type(next_id) is not int or next_id <= highest_update_id:
            return False
    digests = [r.get("digest") for r in replicas.values()]
    expected = [r.get("digest") for r in winning_reply["replicas"].values()]
    return bool(digests[0]) and len(set(digests)) == 1 and digests[0] == expected[0]


def demo_conflicting_updates(doc_id, show_wire=False, evidence_file=None):
    """Run two competing CLIENT PROCESSES; check replies plus final sync state.

    Each update acquires the elected leader's distributed lock (gateway-8000).
    Client checks prove total order and replica agreement; LOCK GRANTED /
    LOCK RELEASED lines in the leader's index log show mutual exclusion.
    """
    if type(doc_id) is not int or doc_id < 0:
        log.error("Document ID must be a non-negative integer.")
        return 1
    print(f"\nM4 contention demo: 2 client processes updating doc_id={doc_id}")
    print("Each writes a different version of the same shared document.")
    print("The gateway acquires the distributed lock (gateway-8000-<token>) for each update.")
    record = {
        "scenario": "two_client_write_contention", "captured_at_utc": utc_now(),
        "document_id": doc_id, "gateway": f"{HOST}:{PORT}",
        "clients": [], "checks": {},
        "limitations": "Client checks verify ordered update_ids and replica agreement. "
                       "Pair with leader logs showing LOCK GRANTED/RELEASED for gateway-8000-<token>."
    }
    children = []
    try:
        for tag in ("A", "B"):
            cmd = [sys.executable, os.path.abspath(__file__), "--update", str(doc_id),
                   f"Milestone 4 Version {tag}", f"Concurrent write from client {tag}",
                   "--gateway-host", HOST, "--gateway-port", str(PORT), "--result-json"]
            if show_wire:
                cmd.append("--show-wire")
            children.append((tag, subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                                   stderr=subprocess.STDOUT, text=True)))
    except OSError as exc:
        log.error("Could not launch independent clients: %s", exc)
        for _, child in children:
            child.kill()
            child.communicate()
        record["checks"]["clients_started"] = False
        write_evidence(evidence_file, record)
        return 1

    responses = []
    for tag, child in children:
        output, _ = child.communicate()
        print(f"\n--- Client {tag} (pid={child.pid}) ---")
        # Keep the useful human-readable display, but exclude the machine marker.
        print("\n".join(line for line in output.rstrip().splitlines()
                        if not line.startswith("M4_RESULT_JSON=")))
        response = parse_child_update(output)
        item = {"client": tag, "pid": child.pid, "exit_code": child.returncode,
                "update_id": response.get("update_id") if response else None,
                "gateway_reply": response}
        record["clients"].append(item)
        if child.returncode == 0 and verify_group_reply(response):
            responses.append(response)

    ids = [r["update_id"] for r in responses]
    record["checks"]["two_successful_acknowledged_updates"] = len(ids) == 2
    record["checks"]["distinct_total_order_ids"] = len(ids) == 2 and len(set(ids)) == 2

    # A separate request checks the state AFTER both child processes have exited.
    final = send_status() if len(ids) == 2 else None
    record["final_status"] = final
    record["checks"]["final_replica_and_shard_agreement"] = False
    if len(ids) == 2 and len(set(ids)) == 2:
        winner = max(responses, key=lambda r: r["update_id"])
        record["checks"]["final_replica_and_shard_agreement"] = verify_final_status(
            final, winner["update_id"], winner)
    passed = all(record["checks"].values())
    record["client_verification_passed"] = passed
    print("\nClient verification:")
    for name, value in record["checks"].items():
        print(f"  {name}: {'PASS' if value else 'FAIL'}")
    if passed:
        print("  Ordered update IDs:", sorted(ids))
        print("  After both updates: three replicas agree and all six members are caught up.")
        print("  Distributed lock: look for LOCK GRANTED/RELEASED to gateway-8000-<token>")
        print("  in the elected leader's log (usually logs/index-8003.log while it leads).")
    else:
        print("  RESULT: not verified. Inspect gateway and replica logs.")
    write_evidence(evidence_file, record)
    return 0 if passed else 1


def current_leader(response):
    """Extract a real elected leader ID from the agreed M4 status response."""
    if not isinstance(response, dict) or response.get("msg_type") != "coord_status_result":
        return None
    leader = response.get("leader")
    if not isinstance(leader, dict):
        return None
    ident = leader.get("id")
    return ident if isinstance(ident, str) and ident.strip() else None


def observe_leader_change(seconds, evidence_file=None, poll_seconds=1.0):
    """Watch an M4 gateway report a DIFFERENT leader, then test read availability.

    The index nodes run the Bully election; the gateway reports the leader
    through coord_status. This client only watches; it never kills a leader.
    """
    if seconds <= 0:
        log.error("Observation time must be greater than zero.")
        return 1
    record = {"scenario": "leader_change_observation", "captured_at_utc": utc_now(),
              "gateway": f"{HOST}:{PORT}", "observations": [], "checks": {},
              "limitations": "Requires M4 leader-election service and server election logs; "
                             "this observer does not elect or terminate a node."}
    baseline = send_coord_status()
    original = current_leader(baseline)
    if original is None:
        print("\nNo M4 elected leader reported. The gateway must support 'coord_status'.")
        record["checks"]["baseline_available"] = False
        write_evidence(evidence_file, record)
        return 1
    print(f"\nCurrent elected leader: {original}. Monitoring for up to {seconds:g} seconds.")
    print("Stop the leader from a SEPARATE terminal using your team's failover plan.")
    record["observations"].append({"at_utc": utc_now(), "leader": original,
                                    "response": baseline})
    deadline = time.monotonic() + seconds
    replacement = None
    try:
        while time.monotonic() < deadline:
            time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))
            status = send_coord_status()
            candidate = current_leader(status)
            # A temporary unavailable coordinator isn't proof of any election.
            if candidate is None:
                continue
            if candidate != original:
                replacement = candidate
                record["observations"].append({"at_utc": utc_now(),
                                                "leader": candidate, "response": status})
                break
    except KeyboardInterrupt:
        print("\nObserver interrupted.")
    record["checks"]["leader_changed"] = replacement is not None
    if replacement:
        print(f"Leader changed: {original} -> {replacement}")
        response = send_request("distributed systems")
        record["post_change_search"] = response
        record["checks"]["search_after_change"] = isinstance(response, dict) \
            and isinstance(response.get("results"), list) and "error" not in response
        print("Search after leader change:",
              "PASS" if record["checks"]["search_after_change"] else "FAIL")
    else:
        print("No replacement leader observed. Check election or increase the observation window.")
    passed = all(record["checks"].values())
    record["client_verification_passed"] = passed
    write_evidence(evidence_file, record)
    if passed:
        print("Client observed leader replacement and a successful search request.")
        print("The election itself is in logs/index-800x.log (ELECTION and I AM THE LEADER lines).")
    return 0 if passed else 1


def display_error(response):
    """Print errors returned by the gateway. True if one was present."""
    if response is None:
        return True  # A detailed connection/protocol error was already logged.
    if "error" in response:
        print(f"\nServer Error ({response.get('error_type', 'unknown')}): {response['error']}")
        return True
    return False


def display_results(response):
    """Display the ranked search matches without altering search results."""
    if display_error(response):
        return
    results = response.get("results")
    if not isinstance(results, list):
        log.error("Gateway search response has no valid results list.")
        return

    print("\nSearch Results")
    print("--------------")
    if not results:
        print("No matching documents found.")
    for number, result in enumerate(results, start=1):
        if not isinstance(result, dict):
            log.error("Invalid result at position %d: %r", number, result)
            continue
        print(f"{number}. {result.get('title', 'Untitled')}")
        print(f"   Document ID: {result.get('doc_id', 'Unknown')}")
        print(f"   Score: {result.get('score', 0)}")


def display_members(members, heading):
    """Display acknowledgments and digest summaries for one node group."""
    print(f"\n{heading}:")
    if not isinstance(members, dict):
        print("  No node status available.")
        return
    for port, info in sorted(members.items()):
        if not isinstance(info, dict):
            print(f"  {port}: invalid response")
            continue
        status = info.get("status", "unknown")
        details = [f"status={status}"]
        for key in ("doc_count", "digest", "next_update_id", "events_received"):
            if info.get(key) is not None:
                details.append(f"{key}={info[key]}")
        if info.get("error"):
            details.append(f"error={info['error']}")
        print(f"  {port}: " + ", ".join(details))


def display_update_result(response):
    """Summarize the gateway's group multicast and pub-sub outcome."""
    if display_error(response):
        return
    if response.get("msg_type") != "update_result":
        log.error("Unexpected update response: %s", response)
        return
    print("\nIndex Update Result")
    print("-------------------")
    print("Update ID:", response.get("update_id", "Unknown"))
    print("Owner shard port:", response.get("owner_shard", "Unknown"))
    display_members(response.get("shards"), "Index shards (group communication)")
    display_members(response.get("replicas"), "Replicas (group communication)")
    print("\nPub-sub event delivered to:", response.get("event_delivered_to", "Unknown"), "subscriber(s)")
    print("All members synchronized:", "YES" if response.get("in_sync") else "NO")
    if "p2p_updated" in response:
        print("Peer-to-peer overlay updated:", "YES" if response["p2p_updated"] else "NO (no peers running)")
    print("Note: The broker's delivery count records successful sends, not durable receipt.")


def display_status(response):
    """Show each member's state and whether the gateway considers it in sync."""
    if display_error(response):
        return
    if response.get("msg_type") != "status_result":
        log.error("Unexpected status response: %s", response)
        return
    print("\nSynchronization Status")
    print("----------------------")
    display_members(response.get("shards"), "Index shards")
    display_members(response.get("replicas"), "Replicas")
    print("\nAll members synchronized:", "YES" if response.get("in_sync") else "NO")


def path_text(path, owner):
    """Peers a lookup passed through, ending at the owner of the key."""
    ports = [f"{p['port']} (id {p['id']})" for p in path]
    if not path or path[-1]["id"] != owner["id"]:
        ports.append(f"{owner['port']} (id {owner['id']})")
    return " -> ".join(ports)


def display_p2p_search(response):
    """Show how each query word was routed through the ring, then the results."""
    if display_error(response):
        return
    if response.get("msg_type") != "p2p_search_result":
        log.error("Unexpected P2P search response: %s", response)
        return
    print(f"\nPeer-to-peer search (entry peer {response.get('entry_peer')})")
    for lookup in response.get("lookups", []):
        state = "found" if lookup.get("found") else "no such term"
        print(f"  '{lookup['word']}' key id {lookup['key_id']}: "
              f"{path_text(lookup['path'], lookup['owner'])}  ({state})")
    display_results({"results": response.get("results", [])})


def display_p2p_ring(response):
    """List the peers in ring order with their neighbours and key counts."""
    if display_error(response):
        return
    if response.get("msg_type") != "p2p_ring_result":
        log.error("Unexpected P2P ring response: %s", response)
        return
    members = response.get("members", [])
    print(f"\nPeer-to-peer ring ({len(members)} peers, clockwise, found through peer {response.get('entry_peer')})")
    print(f"  {'port':<6} {'id':<4} {'predecessor':<12} {'successor':<10} {'keys':<5} owned")
    for m in members:
        print(f"  {m['port']:<6} {m['id']:<4} {str(m['predecessor']):<12} {str(m['successor']):<10} "
              f"{m['keys_total']:<5} {m['keys_owned']}")


def interactive_menu(show_wire=False):
    """Give the user access to search, update, and status in one client."""
    print("===============================")
    print(" Distributed Search Engine M4")
    print("===============================")

    while True:
        print("\n1. Search documents")
        print("2. Add or update a document")
        print("3. Check synchronization status")
        print("4. Search through the peer-to-peer overlay")
        print("5. Show the peer-to-peer ring")
        print("6. Show coordination / elected leader status (M4)")
        print("7. Demonstrate conflicting document updates (M4)")
        print("8. Observe leader replacement (M4; crash the leader from another terminal)")
        print("9. Exit")
        try:
            choice = input("Choose an option (1-9): ").strip()
            if choice == "1":
                display_results(send_request(input("Enter a search query: "), show_wire))
            elif choice == "2":
                id_text = input("Enter document ID (integer): ").strip()
                try:
                    doc_id = int(id_text)
                except ValueError:
                    log.error("Document ID must be an integer.")
                    continue
                title = input("Enter document title: ")
                body = input("Enter document body: ")
                display_update_result(send_update(doc_id, title, body, show_wire))
            elif choice == "3":
                display_status(send_status(show_wire))
            elif choice == "4":
                display_p2p_search(send_p2p_search(input("Enter a search query: "), show_wire))
            elif choice == "5":
                display_p2p_ring(send_p2p_ring(show_wire))
            elif choice == "6":
                display_coord_status(send_coord_status(show_wire))
            elif choice == "7":
                try:
                    doc_id = int(input("Enter document ID to update concurrently: ").strip())
                except ValueError:
                    log.error("Document ID must be an integer.")
                    continue
                demo_conflicting_updates(doc_id, show_wire)
            elif choice == "8":
                try:
                    seconds = float(input("Observe leader changes for how many seconds? ").strip())
                except ValueError:
                    log.error("Please enter a positive number of seconds.")
                    continue
                observe_leader_change(seconds)
            elif choice == "9":
                print("Client exited.")
                break
            else:
                print("Invalid option; choose 1 through 9.")
        except (EOFError, KeyboardInterrupt):
            print("\nClient exited.")
            break


def main():
    global HOST, PORT
    parser = argparse.ArgumentParser(description="Distributed search engine Milestone 4 client")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--search", metavar="QUERY", help="submit one search query and exit")
    action.add_argument("--update", nargs=3, metavar=("DOC_ID", "TITLE", "BODY"),
                        help="submit one document update and exit")
    action.add_argument("--status", action="store_true", help="show replica/shard status and exit")
    action.add_argument("--p2p-search", metavar="QUERY", help="search through the peer-to-peer overlay and exit")
    action.add_argument("--p2p-ring", action="store_true", help="show the peers in the overlay ring and exit")
    action.add_argument("--coord-status", action="store_true", help="show the elected leader and the lock status of the index nodes")
    action.add_argument("--demo-conflict", type=int, metavar="DOC_ID",
                        help="launch two independent clients to update the same document")
    action.add_argument("--observe-leader", type=float, metavar="SECONDS",
                        help="watch for a leader change (crash the leader from another terminal), then check search still works")
    parser.add_argument("--evidence-file", metavar="PATH",
                        help="save actual client-observed conflict/failover evidence as JSON")
    parser.add_argument("--result-json", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--gateway-host", default=HOST, help="gateway host (default: 127.0.0.1)")
    parser.add_argument("--gateway-port", type=int, default=PORT, help="gateway TCP port (default: 8000)")
    parser.add_argument("--show-wire", action="store_true",
                        help="show the Python dictionary, marshalled bytes, and unmarshalled result")
    args = parser.parse_args()
    HOST, PORT = args.gateway_host, args.gateway_port
    if not (1 <= PORT <= 65535):
        parser.error("--gateway-port must be between 1 and 65535")

    if args.evidence_file and args.demo_conflict is None and args.observe_leader is None:
        parser.error("--evidence-file requires --demo-conflict or --observe-leader")
    if args.result_json and args.update is None:
        parser.error("--result-json requires --update")
    if args.search is not None:
        response = send_request(args.search, args.show_wire)
        display_results(response)
        return 0 if response is not None and "error" not in response else 1
    if args.update is not None:
        try:
            doc_id = int(args.update[0])
        except ValueError:
            parser.error("DOC_ID must be an integer")
        response = send_update(doc_id, args.update[1], args.update[2], args.show_wire)
        display_update_result(response)
        if args.result_json:
            print("M4_RESULT_JSON=" + json.dumps(response))
        return 0 if response is not None and response.get("msg_type") == "update_result" \
            and "error" not in response else 1
    if args.status:
        response = send_status(args.show_wire)
        display_status(response)
        return 0 if response is not None and "error" not in response else 1
    if args.p2p_search is not None:
        response = send_p2p_search(args.p2p_search, args.show_wire)
        display_p2p_search(response)
        return 0 if response is not None and "error" not in response else 1
    if args.p2p_ring:
        response = send_p2p_ring(args.show_wire)
        display_p2p_ring(response)
        return 0 if response is not None and "error" not in response else 1
    if args.coord_status:
        response = send_coord_status(args.show_wire)
        display_coord_status(response)
        return 0 if response is not None and response.get("msg_type") == "coord_status_result" and "error" not in response else 1
    if args.demo_conflict is not None:
        return demo_conflicting_updates(args.demo_conflict, args.show_wire, args.evidence_file)
    if args.observe_leader is not None:
        return observe_leader_change(args.observe_leader, args.evidence_file)
    interactive_menu(args.show_wire)
    return 0


if __name__ == "__main__":
    sys.exit(main())