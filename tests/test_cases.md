cat > tests/test_cases.md <<'EOF'
# Test Cases

Run every command from the repository root. Use `python` on Windows and `python3` on macOS or Linux.
The automated tests start their own services, so stop `run_all` first (ports 8000-8003, 8011-8013 and 9000 must be free).

## Milestone 1: search

| # | Test | How | Expected result |
|---|------|-----|-----------------|
| 1 | Search round trip | `python3 tests/test_integration.py` | Reply has the same `request_id` and at least one result |
| 2 | One shard down | Same test, second part (port 8002 is stopped) | Gateway still returns results from the other shards |
| 3 | Empty query | Client menu option 1, press Enter with no text | Client refuses to send it and shows a message |

## Milestone 2: communication

| # | Test | How | Expected result |
|---|------|-----|-----------------|
| 4 | Client unit tests (marshalling, request building, bad replies) | `python3 tests/test_client_m2.py` | 7 tests pass |
| 5 | Marshalling before/after | `python3 scripts/demo_marshalling.py` | Shows the dict, the bytes on the wire, and the dict again, with "identical to original: True" |
| 6 | Wire view on a real request | `python3 client/client.py --search "distributed systems" --show-wire` | Prints the JSON sent and received |
| 7 | Request-reply | Start `bash scripts/run_all.sh`, pick 1, search `distributed` | Numbered results come back |
| 8 | Group message + sync | In another terminal: `python3 client/client.py --update 16 "Raft" "Leader election and log replication"` | 6 members ack (3 shards, 3 replicas), "All members synchronized: YES" |
| 9 | Publisher to many subscribers | Same update as test 8 | "Pub-sub event delivered to: 6 subscriber(s)" |
| 10 | New document is searchable | `python3 client/client.py --search "raft"` | Doc 16 is in the results |
| 11 | Status check | `python3 client/client.py --status` | Same digest on all three replicas, "All members synchronized: YES" |
| 12 | Full sync test | `python3 tests/test_sync.py` | Ends with `ALL SYNC TESTS PASSED` (covers baseline, update, edit, crash, catch-up) |
| 13 | Replica crash and recovery | Stop the process on 8013, send an update (shows 8013 `failed`, "synchronized: NO"), restart `python3 replica/replica.py 8013`, send another update | 8013 catches up, all digests match, "synchronized: YES" |
| 14 | Single index node | Start `python3 index_node/index_node.py 8001`, then `python3 tests/test_index_node.py` (do not run the real broker) | Ack, ordering, duplicate and broker-event checks pass |
EOF