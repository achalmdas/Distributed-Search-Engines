cat > README.md <<'EOF'
# Distributed-Search-Engines
CECS 327 - Introduction to Networks and Distributed Computing  
Milestone 1: Architecture and Models  
Milestone 2: Communication Mechanisms

## Project Overview

This is a small distributed search engine. A client sends a request to a Gateway (the main server). For a search, the Gateway asks three Index Node shards at the same time, merges their matches and returns the top results. For an index update, the Gateway acts as the coordinator of a group of six members (3 shards and 3 replicas), sends the update to all of them, and checks that they all end up with the same data. It also publishes the update through a pub-sub broker.

Milestone 2 adds these communication mechanisms:

- **Request-reply** over TCP sockets (client to gateway, gateway to shards and replicas).
- **Marshalling and unmarshalling** of every message as JSON (`common/protocol.py`).
- **Publish-subscribe** through a broker on port 9000, topic `index.updates`.
- **Group communication**: an index update is multicast to 6 members in one total order (`update_id`), each member acks, and a member that missed updates is caught up.
- **Timestamped logging** in `logs/`.

## Required Software

- Python 3.10 or newer.
- A terminal. Windows uses `python`, macOS and Linux use `python3`.

## How to Install Dependencies

Nothing to install. The project only uses the Python standard library, so `requirements.txt` is empty.

Check your Python version:

```bash
python3 --version
```

On macOS you can use a virtual environment if you like (`python3 -m venv .venv` then `source .venv/bin/activate`), but it is not required.

## Go to the Project Folder First

Every command in this README must be run from the repository root, the folder named `Distributed-Search-Engines`. Open a terminal and move into it (change the path to wherever you cloned or unzipped the project):

```text
Windows (Command Prompt):  cd C:\Users\YourName\Distributed-Search-Engines
macOS or Linux:            cd ~/Distributed-Search-Engines
GitHub Codespaces:         cd /workspaces/Distributed-Search-Engines
```

Check you are in the right place. You should see `client`, `gateway`, `scripts` and `README.md`:

```text
Windows:         dir
macOS or Linux:  ls
```

## How to Start the Services

There are three ways to start everything. The services are the broker (port 9000), 3 index shards (8001, 8002, 8003), 3 replicas (8011, 8012, 8013) and the gateway (8000).

### Method 1: start each service by hand

Open 9 terminals. Start the broker first and the gateway last, because the gateway talks to everything else.

```text
Terminal 1:  python3 broker/broker.py
Terminal 2:  python3 index_node/index_node.py 8001
Terminal 3:  python3 index_node/index_node.py 8002
Terminal 4:  python3 index_node/index_node.py 8003
Terminal 5:  python3 replica/replica.py 8011
Terminal 6:  python3 replica/replica.py 8012
Terminal 7:  python3 replica/replica.py 8013
Terminal 8:  python3 gateway/gateway.py
Terminal 9:  python3 client/client.py
```

All 3 shards must be running for the gateway to return complete results. You can run the client again, or several times, without restarting anything.

### Method 2: Windows

In the command prompt (in PowerShell, put `.\` in front):

```bat
scripts\run_all.bat
```

It opens a separate window for the broker, each shard, each replica and the gateway, so you can watch each service on its own, and runs the client in the window you started it from. Close the other windows when you are done.

### Method 3: Linux or macOS (including Codespaces)

```bash
bash scripts/run_all.sh
```

It starts the broker, shards, replicas and gateway in the background, then opens the client menu in the same terminal. The services stop when you quit the client.

| Service | Port | Role |
|---------|------|------|
| Gateway | 8000 | Receives client requests, coordinates updates |
| Index shards | 8001, 8002, 8003 | Each stores the documents where `doc_id % 3` equals its shard number |
| Replicas | 8011, 8012, 8013 | Each stores a full copy of all documents |
| Broker | 9000 | Pub-sub, topic `index.updates` |

## How to Run the Demo

Start the services (see above). The client is a menu:

```text
1. Search documents
2. Add or update a document
3. Check synchronization status
4. Exit
```

You can also run one action directly from a second terminal while the services are running:

```bash
python3 client/client.py --search "distributed systems"
python3 client/client.py --update 16 "Raft" "Leader election and log replication"
python3 client/client.py --status
```

Add `--show-wire` to any of them to print each message before marshalling (the Python dict), after marshalling (the bytes on the wire) and after unmarshalling:

```bash
python3 client/client.py --search "distributed systems" --show-wire
```

To see marshalling by itself without any services:

```bash
python3 scripts/demo_marshalling.py
```

### What to look for

1. **Request-reply:** a search returns numbered results with the document title, ID and score.
2. **Group message:** after `--update`, the output lists 6 members (3 shards, 3 replicas), each with `status=ack`, and ends with `All members synchronized: YES`.
3. **Pub-sub:** the same output shows `Pub-sub event delivered to: 6 subscriber(s)`.
4. **Sync proof:** every replica reports the same `digest`, and each shard's digest matches the replicas' record for that shard. `--status` shows this at any time.
5. **New data is searchable:** after adding document 16, `--search "raft"` returns it.
6. **Failure and recovery:** stop one replica (for example the process on 8013) and send an update. It shows that replica as `failed` and `All members synchronized: NO`. Restart it with `python3 replica/replica.py 8013` and send another update. The replica catches up on what it missed and the output goes back to `YES`.

The helper scripts `scripts/send_update.py` and `scripts/status.py` do the same as `--update` and `--status` but print the raw JSON.

## How to Reproduce the Tests

Each test starts its own processes, so stop `run_all` first. Ports 8000-8003, 8011-8013 and 9000 must be free.

```bash
python3 tests/test_client_m2.py       # 7 client tests (marshalling, request building, bad replies)
python3 tests/test_integration.py     # Milestone 1: search round trip and one shard down
python3 tests/test_sync.py            # Milestone 2: updates, acks, pub-sub, digests, crash and catch-up
```

`test_integration.py` starts all 3 shards and the gateway, sends a query, checks the response, then kills one shard mid-run and confirms the gateway still returns results from the others. `test_sync.py` starts every service, sends updates, checks that all 6 members ack and their digests match, then crashes a shard and a replica and checks that they catch up after a restart.

`test_sync.py` ends with `ALL SYNC TESTS PASSED`. `test_integration.py` ends with `ALL TESTS PASSED`.

`tests/test_index_node.py` tests a single node with a stand-in broker. Do not run the real broker for it. Start one node first, then run the test:

```bash
python3 index_node/index_node.py 8001
python3 tests/test_index_node.py
```

The full list of test cases and expected results is in `tests/test_cases.md`. Logs from every run are written to `logs/`.

## How to See the Logs

Every service writes a timestamped log to the `logs` folder, one file per service (`gateway.log`, `client.log`, `broker-9000.log`, `index-8001.log`, `replica-8011.log`, and so on). The logs only exist after you have run the services, and they are not committed to git.

```bash
ls logs
cat logs/gateway.log
tail -f logs/gateway.log        # watch live while you run an update (Ctrl+C to stop)
```

On Windows, use `dir logs`, `type logs\gateway.log` and `Get-Content logs\gateway.log -Wait` (PowerShell).

For the demo, the gateway log shows an update going to all 6 members and the acks coming back, the broker log shows the event being published and delivered, and the replica logs show the update and the pub-sub event arriving.

## Message Format and Marshalling

Every message is one JSON object followed by a newline (`\n`), sent as UTF-8 over a TCP socket. The receiver reads until it sees the newline, which is how it knows where the message ends.

- **Marshal:** Python dict, then `json.dumps`, then add `\n`, then encode to UTF-8 bytes.
- **Unmarshal:** decode, strip the newline, `json.loads`, then check that it is a JSON object.
- Messages bigger than 1,000,000 bytes, bad JSON and non-object JSON raise a `ProtocolError`.

The functions are in `common/protocol.py`. Messages made with `make_message` also carry a `msg_type` and a UTC timestamp `ts`.

Search request and reply:

```json
{"request_id": "1234", "query": "distributed systems"}
{"request_id": "1234", "results": [{"doc_id": 1, "title": "Introduction to Distributed Systems", "score": 3}]}
```

Index update (gateway to each member) and the member's ack:

```json
{"msg_type": "index_update", "update_id": 1, "doc": {"doc_id": 16, "title": "Raft", "body": "Leader election and log replication"}}
{"msg_type": "ack", "update_id": 1, "doc_count": 16, "digest": "a0eb543c3863", "next_update_id": 2}
```

## System Components

| File | What it does |
|------|--------------|
| `client/client.py` | Menu and command line client. Sends search, update and status requests, can show the wire format, logs everything. |
| `gateway/gateway.py` | Listens on 8000, one thread per client. Search goes to all three shards in parallel and results are merged. Update and status requests go to the coordinator. |
| `gateway/coordinator.py` | Group communication. Gives each update an `update_id`, sends it to the 6 members, collects acks, replays missed updates, compares digests, and publishes the event to the broker. |
| `index_node/index_node.py` | One shard. Scores documents by counting query words, applies updates in `update_id` order (early ones wait in a holdback buffer), subscribes to the broker. Uses 4 worker threads. |
| `replica/replica.py` | Full copy of all documents. Applies updates, acks with its digest, subscribes to the broker. |
| `broker/broker.py` | Pub-sub broker. Subscribers connect and stay connected, publishers send an event and the broker forwards it to every subscriber. |
| `common/protocol.py` | Marshalling, unmarshalling, framing, `request_reply`, and the content digest. |
| `common/logger.py` | Timestamped logging to `logs/<name>.log` and the console. |

### How sync is checked

A digest is the first 12 characters of a SHA-256 hash over all documents sorted by `doc_id`. The three replicas must have the same digest, and each shard's digest must equal the replicas' stored digest for that shard. `in_sync` is true only if every member acked and these digests match. A member that is down is reported as `failed`, and it is brought up to date from its `next_update_id` the next time an update is sent.

### Delivery behavior

Updates are delivered to every member and acked (reliable, in one total order). Pub-sub is at-most-once: the broker's delivery count shows successful sends, not durable receipt, so the update path through the gateway is what guarantees consistency.
EOF