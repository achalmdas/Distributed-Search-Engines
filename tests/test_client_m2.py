"""Client-specific unit tests; no gateway, broker, or shards need to be running.

Run from repository root: python3 -m unittest tests/test_client_m2.py
(or: python3 tests/test_client_m2.py)
"""
import os
import socket
import sys
import threading
import unittest
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from client import client
from common.protocol import marshal, recv_bytes, unmarshal


class ClientMilestone2Tests(unittest.TestCase):
    def test_search_request_uses_existing_format(self):
        with patch.object(client, "exchange_message", return_value={"ok": True}) as exchange:
            self.assertEqual(client.send_request("  distributed systems  "), {"ok": True})
            request, show_wire = exchange.call_args.args
            self.assertEqual(request["query"], "distributed systems")
            self.assertIsInstance(request["request_id"], str)
            self.assertNotIn("msg_type", request)
            self.assertFalse(show_wire)

    def test_update_request_matches_gateway(self):
        with patch.object(client, "exchange_message", return_value={"ok": True}) as exchange:
            client.send_update(16, "Raft", "Consensus algorithm")
            request, _ = exchange.call_args.args
            self.assertEqual(request["msg_type"], "update")
            self.assertEqual(request["doc"], {
                "doc_id": 16, "title": "Raft", "body": "Consensus algorithm"})
            self.assertIsInstance(request["request_id"], str)

    def test_status_request_matches_gateway(self):
        with patch.object(client, "exchange_message", return_value={"ok": True}) as exchange:
            client.send_status()
            request, _ = exchange.call_args.args
            self.assertEqual(request["msg_type"], "status")
            self.assertIn("request_id", request)

    def test_invalid_inputs_do_not_send(self):
        with patch.object(client, "exchange_message") as exchange:
            self.assertIsNone(client.send_request("  "))
            self.assertIsNone(client.send_update(-1, "Title", "Body"))
            self.assertIsNone(client.send_update(16, "  ", "Body"))
            self.assertIsNone(client.send_update(16, "Title", "  "))
            exchange.assert_not_called()

    def fake_gateway(self, response_fn):
        """Listen once on an OS-chosen port, returning the background thread."""
        server = socket.socket()
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        errors = []

        def worker():
            try:
                conn, _ = server.accept()
                with conn:
                    wire = recv_bytes(conn)
                    self.assertTrue(wire.endswith(b"\n"))
                    request = unmarshal(wire)
                    conn.sendall(response_fn(request))
            except Exception as error:
                errors.append(error)
            finally:
                server.close()

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        return port, thread, errors

    def test_live_tcp_json_request_reply(self):
        port, thread, errors = self.fake_gateway(
            lambda request: marshal({"request_id": request["request_id"], "results": []}))
        with patch.object(client, "PORT", port):
            reply = client.send_request("distributed systems", show_wire=True)
        thread.join(timeout=3)
        self.assertFalse(thread.is_alive())
        self.assertFalse(errors)
        self.assertEqual(reply["results"], [])

    def test_mismatched_reply_is_rejected(self):
        port, thread, errors = self.fake_gateway(
            lambda request: marshal({"request_id": "wrong-id", "results": []}))
        with patch.object(client, "PORT", port):
            self.assertIsNone(client.send_request("test"))
        thread.join(timeout=3)
        self.assertFalse(errors)

    def test_invalid_json_reply_is_rejected(self):
        port, thread, errors = self.fake_gateway(lambda request: b"this is not JSON\n")
        with patch.object(client, "PORT", port):
            self.assertIsNone(client.send_request("test"))
        thread.join(timeout=3)
        self.assertFalse(errors)


if __name__ == "__main__":
    unittest.main()
