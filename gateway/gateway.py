"""
Main Server / Gateway
CECS 327 - Distributed Search Engine, Milestone 1

The gateway is the middle process in this communication path:

    Client (127.0.0.1:8000) -> Gateway -> Index Nodes (127.0.0.1:8001 - 8003)

Messages are newline-terminated JSON objects. The gateway validates each client
request, forwards it to the index node, and returns the node's response to the
client. Every received, forwarded, and returned message is logged.

Run from the repository root:

    python3 gateway/gateway.py
"""

import json
import logging
import socket
import threading
import os
from typing import Any


GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 8000

INDEX_NODES = [
    ("127.0.0.1", 8001),
    ("127.0.0.1", 8002),
    ("127.0.0.1", 8003),
]

SOCKET_TIMEOUT_SECONDS = 5
MAX_MESSAGE_BYTES = 1_000_000


LOG_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "logs"
)

os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(
    LOG_DIR,
    "gateway.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

log = logging.getLogger("gateway-8000")


class ClientRequestError(Exception):
    """Raised when the client sends an invalid request."""


class IndexNodeError(Exception):
    """Raised when the index node sends an invalid response."""


def receive_json_message(connection: socket.socket) -> dict[str, Any]:
    """Receive one newline-terminated JSON object from a TCP connection."""
    data = bytearray()

    while not data.endswith(b"\n"):
        chunk = connection.recv(4096)
        if not chunk:
            break

        data.extend(chunk)
        if len(data) > MAX_MESSAGE_BYTES:
            raise ValueError("message exceeds the maximum allowed size")

    if not data:
        raise ValueError("connection closed before a message was received")

    message = json.loads(data.decode("utf-8"))
    if not isinstance(message, dict):
        raise ValueError("message must be a JSON object")

    return message


def send_json_message(connection: socket.socket, message: dict[str, Any]) -> None:
    """Send one dictionary as newline-terminated JSON."""
    encoded_message = (json.dumps(message) + "\n").encode("utf-8")
    connection.sendall(encoded_message)


def validate_client_request(request: dict[str, Any]) -> dict[str, str]:
    """Validate and normalize the request expected from client/client.py."""
    request_id = request.get("request_id")
    query = request.get("query")

    if not isinstance(request_id, str) or not request_id.strip():
        raise ClientRequestError("request_id must be a non-empty string")

    if not isinstance(query, str) or not query.strip():
        raise ClientRequestError("query must be a non-empty string")

    return {"request_id": request_id, "query": query.strip()}


def validate_index_response(
    response: dict[str, Any], expected_request_id: str
) -> dict[str, Any]:
    """Ensure the backend response belongs to this request and is usable."""
    if response.get("request_id") != expected_request_id:
        raise IndexNodeError("index node returned a mismatched request_id")

    if "error" in response:
        raise IndexNodeError(str(response["error"]))

    if not isinstance(response.get("results"), list):
        raise IndexNodeError("index node response is missing a results list")

    return response


def query_index_node(
    request: dict[str, str],
    host: str,
    port: int
) -> dict[str, Any]:
    """Send a request to one index shard."""

    log.info(
        "FORWARD to index node %s:%d: %s",
        host,
        port,
        request,
    )

    with socket.create_connection(
        (host, port),
        timeout=SOCKET_TIMEOUT_SECONDS
    ) as index_socket:

        index_socket.settimeout(SOCKET_TIMEOUT_SECONDS)

        send_json_message(index_socket, request)

        try:
            response = receive_json_message(index_socket)

        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:
            raise IndexNodeError(
                f"index node {port} returned invalid JSON: {error}"
            ) from error

    log.info(
        "RECEIVED from index node %s:%d: %s",
        host,
        port,
        response,
    )

    return validate_index_response(
        response,
        request["request_id"]
    )

def query_all_index_nodes(request: dict[str, str]) -> dict[str, Any]:
    """Query all index shards in parallel and merge their results."""

    results = []
    threads = []
    lock = threading.Lock()

    def query_node(host, port):
        try:
            response = query_index_node(
                request,
                host,
                port
            )

            with lock:
                results.extend(response["results"])

        except Exception as error:
            log.error(
                "Shard %s:%d failed: %s",
                host,
                port,
                error
            )

    # Start all shard requests at approximately the same time
    for host, port in INDEX_NODES:

        thread = threading.Thread(
            target=query_node,
            args=(host, port)
        )

        thread.start()
        threads.append(thread)

    # Wait until all shards have answered
    for thread in threads:
        thread.join()

    # Sort combined results by score
    results.sort(
        key=lambda result: result.get("score", 0),
        reverse=True
    )

    # Return one combined response
    return {
        "request_id": request["request_id"],
        "results": results[:10]
    }


def make_error_response(
    request_id: str | None, error: str, error_type: str
) -> dict[str, Any]:
    """Create a consistent error response that client.py can display."""
    return {
        "request_id": request_id,
        "error": error,
        "error_type": error_type,
    }


def handle_client(client_socket: socket.socket, client_address: tuple[str, int]) -> None:
    """Handle one client request and return one response."""
    request_id: str | None = None

    with client_socket:
        client_socket.settimeout(SOCKET_TIMEOUT_SECONDS)

        try:
            raw_request = receive_json_message(client_socket)
            log.info("RECEIVED from client %s: %s", client_address, raw_request)

            if isinstance(raw_request.get("request_id"), str):
                request_id = raw_request["request_id"]

            request = validate_client_request(raw_request)
            request_id = request["request_id"]
            response = query_all_index_nodes(request)

        except ClientRequestError as error:
            log.warning("INVALID request from %s: %s", client_address, error)
            response = make_error_response(request_id, str(error), "invalid_request")

        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:
            log.warning("MALFORMED message from %s: %s", client_address, error)
            response = make_error_response(
                request_id, "request must be valid newline-terminated JSON", "invalid_json"
            )

        except (socket.timeout, TimeoutError):
            log.error("TIMEOUT while processing request_id=%s", request_id)
            response = make_error_response(
                request_id, "index node timed out", "backend_timeout"
            )

        except ConnectionRefusedError:
            log.error("INDEX NODE unavailable at %s:%d", INDEX_NODE_HOST, INDEX_NODE_PORT)
            response = make_error_response(
                request_id, "index node is unavailable", "backend_unavailable"
            )

        except IndexNodeError as error:
            log.error("INVALID index node response for request_id=%s: %s", request_id, error)
            response = make_error_response(request_id, str(error), "backend_error")

        except OSError as error:
            log.error("NETWORK error for request_id=%s: %s", request_id, error)
            response = make_error_response(
                request_id, "network error while contacting index node", "network_error"
            )

        except Exception:
            log.exception("UNEXPECTED error for request_id=%s", request_id)
            response = make_error_response(
                request_id, "internal gateway error", "internal_error"
            )

        try:
            send_json_message(client_socket, response)
            log.info("SENT to client %s: %s", client_address, response)
        except OSError as error:
            log.error("Could not send response to client %s: %s", client_address, error)


def main() -> None:
    """Start the gateway and accept client connections until interrupted."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((GATEWAY_HOST, GATEWAY_PORT))
    server_socket.listen()

    log.info(
    "Gateway listening on %s:%d; forwarding to index nodes: %s",
    GATEWAY_HOST,
    GATEWAY_PORT,
    INDEX_NODES,
)

    try:
        while True:
            client_socket, client_address = server_socket.accept()
            threading.Thread(
                target=handle_client,
                args=(client_socket, client_address),
                daemon=True,
            ).start()
    except KeyboardInterrupt:
        log.info("Gateway shutting down")
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()
