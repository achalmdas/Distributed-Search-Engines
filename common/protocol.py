"""
Message protocol (marshalling / unmarshalling) shared by every service.
CECS 327 - Milestone 2

Wire format: one JSON object, UTF-8 encoded, terminated by a newline.

    marshal   : dict -> json.dumps -> text + "\n" -> UTF-8 bytes
    unmarshal : bytes -> UTF-8 decode -> json.loads -> dict -> validate

TCP is a byte stream with no message boundaries, so the newline is the
frame delimiter and receivers read until they see it.
"""

import hashlib
import json
import socket
from datetime import datetime, timezone

MAX_MESSAGE_BYTES = 1_000_000


class ProtocolError(Exception):
    """Raised when a message cannot be unmarshalled or is invalid."""


# ------------------------------------------------------------ marshalling
def marshal(message: dict) -> bytes:
    """Python dict -> bytes ready for the socket."""
    if not isinstance(message, dict):
        raise ProtocolError("message must be a dict")
    return (json.dumps(message) + "\n").encode("utf-8")


def unmarshal(data: bytes) -> dict:
    """Bytes read from the socket -> Python dict."""
    if len(data) > MAX_MESSAGE_BYTES:
        raise ProtocolError("message exceeds the maximum allowed size")
    try:
        message = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError(f"invalid JSON message: {error}") from error
    if not isinstance(message, dict):
        raise ProtocolError("message must be a JSON object")
    return message


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def make_message(msg_type: str, **fields) -> dict:
    """Build a typed message with a UTC timestamp."""
    return {"msg_type": msg_type, "ts": now_iso(), **fields}


# ------------------------------------------------------------ socket helpers
def recv_bytes(connection: socket.socket) -> bytes:
    """Read from the socket until the newline that ends one message."""
    data = bytearray()
    while not data.endswith(b"\n"):
        chunk = connection.recv(4096)
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > MAX_MESSAGE_BYTES:
            raise ProtocolError("message exceeds the maximum allowed size")
    if not data:
        raise ConnectionError("connection closed before a message was received")
    return bytes(data)


def recv_msg(connection: socket.socket) -> dict:
    return unmarshal(recv_bytes(connection))


def send_msg(connection: socket.socket, message: dict) -> None:
    connection.sendall(marshal(message))


def request_reply(host: str, port: int, message: dict, timeout: float = 2.0) -> dict:
    """Open a connection, send one message, return the one reply."""
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        send_msg(s, message)
        return recv_msg(s)


# ------------------------------------------------------------ replica digest
def docs_digest(docs) -> str:
    """Short hash of a set of documents; equal digest = equal contents."""
    ordered = sorted(docs, key=lambda d: d["doc_id"])
    blob = json.dumps(ordered, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:12]