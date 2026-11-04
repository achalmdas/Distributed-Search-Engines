"""
Client (User / Request Side)
CECS 327 - Distributed Search Engine, Milestone 2: Communication

Run from the repository root:
    python3 client/client.py
    python3 client/client.py --search "distributed systems"
    python3 client/client.py --update 16 "Raft Consensus" "leader election and log replication"
    python3 client/client.py --status
    python3 client/client.py --p2p-search "distributed systems"
    python3 client/client.py --p2p-ring
    python3 client/client.py --search "distributed systems" --show-wire

The client talks ONLY to the gateway at 127.0.0.1:8000. The gateway
handles index shards, group replication, publication to the broker, and
(Milestone 3) lookups through the peer-to-peer overlay.
All TCP messages use the shared common.protocol format: one UTF-8 JSON
object terminated by a newline.
"""

import argparse
import os
import socket
import sys
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


def receive_message(client_socket):
    """Read one complete TCP frame and unmarshal the JSON response."""
    return unmarshal(recv_bytes(client_socket))


def exchange_message(request, show_wire=False):
    """Marshal, send a request to the gateway, and receive its reply."""
    try:
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
            log.info("SENT: %s", request)

            wire_response = recv_bytes(client_socket)
            response = unmarshal(wire_response)
            log.info("RECEIVED: %s", response)

        if show_wire:
            print("4. Reply received as bytes:", wire_response)
            print("5. After unmarshalling (Python dictionary):", response)
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
    print(" Distributed Search Engine M3")
    print("===============================")

    while True:
        print("\n1. Search documents")
        print("2. Add or update a document")
        print("3. Check synchronization status")
        print("4. Search through the peer-to-peer overlay")
        print("5. Show the peer-to-peer ring")
        print("6. Exit")
        try:
            choice = input("Choose an option (1-6): ").strip()
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
                print("Client exited.")
                break
            else:
                print("Invalid option; choose 1, 2, 3, 4, 5, or 6.")
        except (EOFError, KeyboardInterrupt):
            print("\nClient exited.")
            break


def main():
    parser = argparse.ArgumentParser(description="Distributed search engine Milestone 3 client")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--search", metavar="QUERY", help="submit one search query and exit")
    action.add_argument("--update", nargs=3, metavar=("DOC_ID", "TITLE", "BODY"),
                        help="submit one document update and exit")
    action.add_argument("--status", action="store_true", help="show replica/shard status and exit")
    action.add_argument("--p2p-search", metavar="QUERY", help="search through the peer-to-peer overlay and exit")
    action.add_argument("--p2p-ring", action="store_true", help="show the peers in the overlay ring and exit")
    parser.add_argument("--show-wire", action="store_true",
                        help="show the Python dictionary, marshalled bytes, and unmarshalled result")
    args = parser.parse_args()

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
        return 0 if response is not None and "error" not in response else 1
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
    interactive_menu(args.show_wire)
    return 0


if __name__ == "__main__":
    sys.exit(main())