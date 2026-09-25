# Distributed-Search-Engines
CECS 327 - Introduction to Networks and Distributed Computing  
Milestone 1: Architecture and Models

## Project Overview

This project is a basic distributed search engine. A client sends a search query to a Main Server / Gateway. The Gateway forwards that request to three Index Node shards, merges their matches, and returns the top results to the Client.

The Milestone 1 prototype demonstrates communication between multiple processes, JSON-based request/response messages, TCP socket communication, logging, and basic error handling.

## Required Software

- Python 3.10 or newer. The Gateway uses syntax that requires this version.
- A terminal that can run Python. On Windows, `scripts/run_all.bat` uses `python`. On macOS or Linux, `scripts/run_all.sh` uses `python3`.

No extra packages are required. The Client, Gateway, and Index Node use only the Python standard library.

## How to Install Dependencies

There is nothing to install beyond Python. This repository has no third-party imports.

Confirm Python is available:

```bash
python --version
```

On macOS or Linux, use `python3 --version` if `python` is not on your PATH. The reported version should be 3.10 or newer.

## How to Start Each Service

Run every command below from the repository root. Start the three Index Nodes first, then the Gateway, then the Client. Each process stays running in its own terminal until you stop it.

Index Node shards. Each process loads `index_node/data/documents.json` and keeps the documents for its shard (`doc_id % 3`).

```bash
python index_node/index_node.py 8001
python index_node/index_node.py 8002
python index_node/index_node.py 8003
```

Port `8001` is the default, so `python index_node/index_node.py` starts only the first shard. The Gateway expects all three ports.

Gateway. It listens on `127.0.0.1:8000` and queries `127.0.0.1` on ports `8001`, `8002`, and `8003`.

```bash
python gateway/gateway.py
```

Client. It connects to `127.0.0.1:8000`, asks for one query, prints the results, and exits.

```bash
python client/client.py
```

On macOS or Linux, replace `python` with `python3` in these commands.

## How to Run the Demo

From the repository root, one script starts all three shards, the Gateway, and the Client.

Windows:

```bat
scripts\run_all.bat
```

The script opens a separate console for each shard, the Gateway, and the Client.

macOS or Linux:

```bash
bash scripts/run_all.sh
```

The script starts the shards and Gateway in the background, runs the Client in the current terminal, and stops the background processes when the Client exits.

When the Client prints `Enter a search query:`, type a query and press Enter. Example:

```text
distributed systems
```

A successful run prints numbered matches. Each match shows the document title, document ID, and score. If nothing matches, the Client prints `No matching documents found.`

You can start the same processes by hand with the commands in [How to Start Each Service](#how-to-start-each-service). Use a separate terminal for each process, and start the Client only after the Gateway is listening.

## How to Reproduce the Test Cases

`tests/test_integration.py` is the automated test. From the repository root:

```bash
python tests/test_integration.py
```

Use `python3` on macOS or Linux if needed. The test starts its own processes, so stop any Gateway or Index Node you already have running on ports `8000`–`8003` before you run it.

The test does two checks:

1. Round trip. It starts shards on ports `8001`, `8002`, and `8003`, starts the Gateway on port `8000`, and sends the query `distributed systems`. It checks that the response has the same `request_id`, includes a `results` list, and returns at least one document.
2. Shard failure. It stops the process on port `8002` and sends `distributed systems` again. It checks that the Gateway still returns a `results` list from the shards that are still up.

A passing run ends with `ALL TESTS PASSED`.

## System Components

### Client
File:
`client/client.py`

The Client:
- Accepts a search query from the user.
- Creates a unique request ID.
- Connects to the Main Server / Gateway using a TCP socket.
- Sends the query as a JSON message.
- Waits for a response.
- Logs sent and received messages.
- Displays returned search results.
- Handles empty queries and basic connection errors.

The Client connects to:

`127.0.0.1:8000`

### Main Server / Gateway
File:
`gateway/gateway.py`

The Gateway:

- Listens for client connections on `127.0.0.1:8000`.
- Validates the client's request ID and search query.
- Forwards each valid request to the Index Nodes on `127.0.0.1:8001`, `127.0.0.1:8002`, and `127.0.0.1:8003`.
- Merges the shard results, sorts them by score, and returns the top 10 to the Client.
- Logs messages received, forwarded, and returned to `logs/gateway.log` and the console.
- Handles invalid requests, connection failures, and timeouts. If one shard fails, results from the other shards are still returned.
- Supports multiple client connections using threads.

### Index Node / Backend Data Service
File:
`index_node/index_node.py`

The Index Node:

- Listens on `0.0.0.0` at the port given on the command line (`8001`, `8002`, or `8003`).
- Loads `index_node/data/documents.json` and serves one shard of that corpus.
- Scores documents by counting query words in the title and body, and returns up to 10 matches with `doc_id`, `title`, and `score`.
- Accepts connections on a main thread and handles them with 4 worker threads.
- Logs received and sent messages to `logs/index-<port>.log` and the console.

## Communication Flow

The Milestone 1 communication flow is:

Client -> Main Server / Gateway -> Index Nodes (ports 8001, 8002, 8003) -> Main Server / Gateway -> Client

Messages are sent over TCP sockets using JSON. Each JSON message is newline-terminated.

Example request:

```json
{
  "request_id": "1234",
  "query": "distributed systems"
}
```

Example response from the Gateway:

```json
{
  "request_id": "1234",
  "results": [
    {"doc_id": 1, "title": "Introduction to Distributed Systems", "score": 3}
  ]
}
```
