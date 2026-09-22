"""
Client
CECS 327 - Distributed Search Engine, Milestone 1

Connects to the gateway, sends one query, prints the ranked results
the gateway sends back, and logs the request/response like the other
two services.

Run:
    python client/client.py "distributed systems"
    python client/client.py "distributed systems" --gateway localhost:7000
"""

import os
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.logger import setup_logging
from common.protocol import send_json, recv_json, connect_with_retry

log = setup_logging("client")


def parse_args():
    args = sys.argv[1:]
    gateway_addr = ("localhost", 7000)
    query_words = []

    i = 0
    while i < len(args):
        if args[i] == "--gateway":
            host, port = args[i + 1].split(":")
            gateway_addr = (host, int(port))
            i += 2
        else:
            query_words.append(args[i])
            i += 1

    query = " ".join(query_words) if query_words else "distributed systems"
    return gateway_addr, query


def main():
    (host, port), query = parse_args()

    request = {
        "request_id": str(uuid.uuid4())[:8],
        "query": query,
    }

    log.info("Connecting to gateway at %s:%d", host, port)
    conn = connect_with_retry(host, port, attempts=5, delay=0.5, log=log)

    try:
        send_json(conn, request)
        log.info("SENT to gateway: %s", request)

        response = recv_json(conn)
        log.info("RECV from gateway: %s", response)
    finally:
        conn.close()

    print(f"\nQuery: \"{query}\"")
    if not response:
        print("No response from gateway.")
        return

    if "error" in response:
        print(f"Gateway returned an error: {response['error']}")
        return

    print(f"Nodes queried: {response.get('nodes_queried')}")
    if response.get("nodes_failed"):
        print(f"Nodes that did NOT respond: {response['nodes_failed']}")

    results = response.get("results", [])
    if not results:
        print("No matching documents.")
        return

    #prints out results
    print(f"\nTop {len(results)} result(s):")
    for i, r in enumerate(results, start=1):
        print(f"  {i}. [doc_id={r['doc_id']}] {r['title']}  (score={r['score']})")


if __name__ == "__main__":
    main()