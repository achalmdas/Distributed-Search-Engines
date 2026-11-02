# Test Cases

Run every command from the repository root. Use `python` on Windows and `python3` on macOS or Linux.
The automated tests start their own services, so stop `run_all` first (ports 8000-8003, 8011-8013, 9000 and 9101-9109 must be free).

## Milestone 1: search

| # | Test | How | Expected result |
|---|------|-----|-----------------|
| 1 | Search round trip | `python3 tests/test_integration.py` | Reply has the same `request_id` and at least one result |
| 2 | One shard down | Same test, second part (port 8002 is stopped) | Gateway still returns results from the other shards |
| 3 | Empty query | Client menu option 1, press Enter with no text | Client refuses to send it and shows a message |

## Milestone 2: communication

| # | Test | How | Expected result |
|---|------|-----|-----------------|
| 4 | Client unit tests (marshalling, request building, bad replies) | `python3 tests/test_client_m2.py` | 9 tests pass |
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

## Milestone 3: peer-to-peer overlay

Stop `run_all` and `run_p2p` first. The test and the demo use ports 9101-9105.

| # | Test | How | Expected result |
|---|------|-----|-----------------|
| 15 | Full P2P test | `python3 tests/test_p2p.py` | Ends with `ALL P2P TESTS PASSED` (about a minute) |
| 16 | Peers start, ring forms | Test 1, or `bash scripts/run_p2p.sh` then `python3 scripts/p2p_cli.py ring 9101` | 4 peers in id order, every successor is the next id, `ring closes back to the first peer: True`, 8 fingers each |
| 17 | Publish into the DHT | `python3 scripts/p2p_cli.py publish 9101` | 15 documents stored as 261 keys, spread over all peers; every key owned by exactly one peer and replicated on its successor |
| 18 | Lookup through the overlay | `python3 scripts/p2p_cli.py lookup 9103 systems` | Prints the hop path (for example 9103 -> 9104 -> 9101) and the owner; same owner from every entry peer |
| 19 | Resource discovery | `python3 scripts/p2p_cli.py search 9104 "distributed systems"` and `python3 scripts/p2p_cli.py get-doc 9102 3` | Same ranking as the normal search; document 3 is found at the peer that owns its key |
| 20 | Unknown word | `python3 scripts/p2p_cli.py search 9101 zzzznotaword` | Empty result, no error |
| 21 | Search through the gateway | Start `run_all`, then `python3 client/client.py --p2p-search "distributed systems"` and `--p2p-ring` | Results and the list of peers come back through the gateway; empty query is refused |
| 22 | Peer joins | `python3 p2p/peer.py 9105 --join 9101`, then `ring` | 5 peers, new peer id 23 owns keys it received from its successor, no key owned twice, searches unchanged |
| 23 | Peer leaves | `python3 scripts/p2p_cli.py leave 9102`, then `ring` and `search` | Peer gone from the ring, no key lost, searches still complete |
| 24 | Peer crashes | Kill one peer, wait about 5 seconds, then `ring` and `search` | Neighbours log `PEER LOST`, ring is whole again, searches still complete from replicas |
| 25 | Duplicate id refused | Start a peer with the id of a running peer (`--id`) | The new peer exits with code 1, ring unchanged |
| 26 | Guided demo | `python3 scripts/p2p_demo.py` | Walks through start, publish, lookup, discovery, join, leave and crash with log excerpts |

