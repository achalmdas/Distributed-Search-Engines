"""Milestone 4 client logical clocks and coordination-request contract.

Run from repository root: python3 tests/test_client_m4.py
"""
import os
import io
import json
import tempfile
from pathlib import Path
import socket
import sys
import threading
import unittest
from unittest.mock import patch
from contextlib import redirect_stdout

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from client import client
from common.protocol import marshal, recv_msg, send_msg


class Milestone4ClientTests(unittest.TestCase):
    def test_send_and_receive_lamport_rules(self):
        clock = client.LamportClock()
        self.assertEqual(clock.send(), 1)
        self.assertEqual(clock.send(), 2)
        self.assertEqual(clock.receive(8), 9)
        self.assertEqual(clock.send(), 10)
        self.assertEqual(clock.receive(2), 11)
        self.assertEqual(clock.receive(), 12)  # legacy response, no remote time

    def test_invalid_lamport_ts_rejected(self):
        clock = client.LamportClock()
        for bad in (-1, True, 2.5, "5"):
            with self.assertRaises(client.ProtocolError):
                clock.receive(bad)

    def test_coord_status_request(self):
        with patch.object(client, "exchange_message", return_value={"ok": True}) as exchange:
            client.send_coord_status(show_wire=True)
            request, show_wire = exchange.call_args.args
            self.assertEqual(request["msg_type"], "coord_status")
            self.assertIn("request_id", request)
            self.assertTrue(show_wire)

    def test_tcp_request_has_lamport_and_receive_updates_clock(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        captured = []
        errors = []

        def fake_gateway():
            try:
                conn, _ = server.accept()
                with conn:
                    req = recv_msg(conn)
                    captured.append(req)
                    send_msg(conn, {
                        "request_id": req["request_id"], "results": [],
                        "sender_id": "gateway-8000", "lamport_ts": 18
                    })
            except Exception as exc:
                errors.append(exc)
            finally:
                server.close()

        worker = threading.Thread(target=fake_gateway, daemon=True)
        worker.start()
        with patch.object(client, "PORT", port), patch.object(client, "CLOCK", client.LamportClock()):
            response = client.send_request("systems")
            clock_after = client.CLOCK.read()
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertFalse(errors)
        self.assertEqual(response["lamport_ts"], 18)
        self.assertEqual(captured[0]["lamport_ts"], 1)
        self.assertEqual(captured[0]["sender_id"], client.CLIENT_ID)
        self.assertEqual(clock_after, 19)


    def test_lamport_clock_thread_safe_under_contention(self):
        clock = client.LamportClock()
        seen = []
        seen_lock = threading.Lock()

        def advance():
            local = [clock.send() for _ in range(50)]
            with seen_lock:
                seen.extend(local)

        workers = [threading.Thread(target=advance) for _ in range(8)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        self.assertEqual(clock.read(), 400)
        self.assertEqual(sorted(seen), list(range(1, 401)))

    @staticmethod
    def update_response(update_id, digest="abc"):
        return {"msg_type": "update_result", "update_id": update_id,
                "in_sync": True,
                "shards": {str(n): {"status": "ack"} for n in (8001, 8002, 8003)},
                "replicas": {str(n): {"status": "ack", "digest": digest}
                             for n in (8011, 8012, 8013)}}

    @staticmethod
    def status_response(next_id=3, digest="abc"):
        return {"msg_type": "status_result", "in_sync": True,
                "shards": {str(n): {"status": "ack", "next_update_id": next_id}
                           for n in (8001, 8002, 8003)},
                "replicas": {str(n): {"status": "ack", "digest": digest,
                                     "next_update_id": next_id}
                             for n in (8011, 8012, 8013)}}

    def test_group_reply_checks_ack_and_digest_agreement(self):
        self.assertTrue(client.verify_group_reply(self.update_response(1)))
        missing_ack = self.update_response(1)
        missing_ack["shards"]["8001"]["status"] = "failed"
        self.assertFalse(client.verify_group_reply(missing_ack))
        mixed_digest = self.update_response(1)
        mixed_digest["replicas"]["8012"]["digest"] = "different"
        self.assertFalse(client.verify_group_reply(mixed_digest))
        self.assertFalse(client.verify_group_reply({"update_id": 2}))
        self.assertFalse(client.verify_group_reply(self.update_response(True)))

    def test_final_status_checks_watermarks_and_last_replica_digest(self):
        self.assertTrue(client.verify_final_status(
            self.status_response(next_id=3), 2, self.update_response(2)))
        self.assertFalse(client.verify_final_status(
            self.status_response(next_id=2), 2, self.update_response(2)))
        self.assertFalse(client.verify_final_status(
            self.status_response(next_id=3, digest="old"), 2, self.update_response(2)))
        mixed = self.status_response(next_id=3)
        mixed["replicas"]["8012"]["digest"] = "out-of-sync"
        self.assertFalse(client.verify_final_status(mixed, 2, self.update_response(2)))

    def test_machine_readable_reply_parsing(self):
        reply = self.update_response(17)
        text = "INFO \nM4_RESULT_JSON=" + json.dumps(reply) + "\n"
        self.assertEqual(client.parse_child_update(text), reply)
        self.assertIsNone(client.parse_child_update("random log line\n"))
        self.assertIsNone(client.parse_child_update("M4_RESULT_JSON=not-json\n"))

    def test_coord_status_requires_actual_leader(self):
        self.assertIsNone(client.current_leader(None))
        self.assertIsNone(client.current_leader({"msg_type": "status_result"}))
        self.assertIsNone(client.current_leader({"msg_type": "coord_status_result", "leader": {}}))
        self.assertEqual(client.current_leader(
            {"msg_type": "coord_status_result", "leader": {"id": "node-3"}}), "node-3")

    def test_conflict_demo_checks_real_replies_and_final_status(self):
        replies = [self.update_response(2, digest="final"),
                   self.update_response(1, digest="first")]
        class FakeChild:
            def __init__(self, reply, pid):
                self.pid = pid
                self.returncode = 0
                self.reply = reply
            def communicate(self):
                return "M4_RESULT_JSON=" + json.dumps(self.reply) + "\n", None
        children = [FakeChild(replies[0], 6001), FakeChild(replies[1], 6002)]
        with tempfile.TemporaryDirectory() as d:
            evidence = str(Path(d) / "evidence.json")
            with patch.object(client.subprocess, "Popen", side_effect=children) as popen, \
                    patch.object(client, "send_status", return_value=self.status_response(
                        next_id=3, digest="final")), redirect_stdout(io.StringIO()):
                result = client.demo_conflicting_updates(16, evidence_file=evidence)
            self.assertEqual(result, 0)
            self.assertEqual(popen.call_count, 2)
            self.assertTrue(Path(evidence).is_file())
            report = json.loads(Path(evidence).read_text())
            self.assertTrue(report["client_verification_passed"])
            self.assertEqual({r["update_id"] for r in report["clients"]}, {1, 2})
            self.assertIn("limitations", report)

    def test_conflict_demo_rejects_duplicate_update_ids(self):
        reply = self.update_response(3)
        class FakeChild:
            pid = 100
            returncode = 0
            def communicate(self):
                return "M4_RESULT_JSON=" + json.dumps(reply) + "\n", None
        with patch.object(client.subprocess, "Popen", side_effect=[FakeChild(), FakeChild()]), \
                patch.object(client, "send_status") as status, redirect_stdout(io.StringIO()):
            result = client.demo_conflicting_updates(16)
        self.assertEqual(result, 1)
        status.assert_called_once()

    def test_observe_leader_change_and_recovered_search(self):
        before = {"msg_type": "coord_status_result", "leader": {"id": "node-3", "term": 1}}
        after = {"msg_type": "coord_status_result", "leader": {"id": "node-2", "term": 2}}
        with tempfile.TemporaryDirectory() as d:
            outfile = str(Path(d) / "failover.json")
            with patch.object(client, "send_coord_status", side_effect=[before, after]), \
                    patch.object(client, "send_request", return_value={"results": []}) as query, \
                    patch.object(client.time, "monotonic", side_effect=[0, 0, 0]), \
                    patch.object(client.time, "sleep"), redirect_stdout(io.StringIO()):
                result = client.observe_leader_change(10, evidence_file=outfile)
            self.assertEqual(result, 0)
            query.assert_called_once_with("distributed systems")
            report = json.loads(Path(outfile).read_text())
            self.assertTrue(report["checks"]["leader_changed"])
            self.assertTrue(report["checks"]["search_after_change"])
            self.assertEqual(len(report["observations"]), 2)

    def test_observer_fails_immediately_without_m4_gateway(self):
        with patch.object(client, "send_coord_status", return_value={"error_type": "invalid_request"}), \
                patch.object(client.time, "sleep") as sleep, redirect_stdout(io.StringIO()):
            result = client.observe_leader_change(10)
        self.assertEqual(result, 1)
        sleep.assert_not_called()

    def test_invalid_observation_time_no_contact(self):
        with patch.object(client, "send_coord_status") as status:
            self.assertEqual(client.observe_leader_change(0), 1)
        status.assert_not_called()


if __name__ == "__main__":
    unittest.main()
