# Distributed-Search-Engines
CECS 327 - Introduction to Networks and Distributed Computing  
Milestone 1: Architecture and Models

## Project Overview

This project is a basic distributed search engine. A client sends a search query to a Main Server / Gateway, which forwards the request to a backend Index Node. The Index Node searches its local document data and returns matching results back through the Gateway to the Client.
The Milestone 1 prototype demonstrates communication between multiple processes, JSON-based request/response messages, TCP socket communication, logging, and basic error handling.

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
- Forwards valid requests to the Index Node on `127.0.0.1:8001`.
- Returns the Index Node's search results to the Client.
- Logs messages received, forwarded, and returned.
- Handles invalid requests, connection failures, and timeouts.
- Supports multiple client connections using threads.

Run from the repository root:

```bash
python3 gateway/gateway.py
```

### Index Node / Backend Data Service
File:
`index_node/index_node.py`

# TODO

## Communication Flow

The intended Milestone 1 communication flow is:

Client -> Main Server / Gateway -> Index Node -> Main Server / Gateway -> Client

Messages are sent over TCP sockets using JSON. Each JSON message is newline-terminated.

Example request:

```json
{
  "request_id": "1234",
  "query": "distributed systems"
}