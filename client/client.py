"""
Client  (User / Request Side)
CECS 327 - Distributed Search Engine, Milestone 1


Design:
  - accepts a search query from the user
  - creates a unique request ID for each search
  - connects to the Main Server / Gateway using a TCP socket
  - sends the search request as JSON
  - waits for the server response
  - displays the returned search results
  - logs every message sent and received

Run:
    python client/client.py

Connection:
    host: 127.0.0.1
    port: 8000

Message format (one JSON object per line, newline-terminated):
    client -> gateway : {"request_id": "...", "query": "distributed systems"}

    gateway -> client : {"request_id": "...",
                         "results": [
                             {"doc_id": 1, "title": "...", "score": 3},
                             ...
                         ]}
"""


import json
import socket
import uuid


# Address of the Main Server / Gateway.
# 127.0.0.1 means the server is running on the same computer.
# MAKE SURE THIS MATCHES THE HOST AND PORT USED BY THE GATEWAY.
HOST = "127.0.0.1"
PORT = 8000


def receive_message(client_socket):
    # Receives one newline-terminated JSON message from the gateway
    # and converts it into a Python dictionary.

    data = b""

    # TCP may deliver a message in multiple pieces, so continue
    # receiving until the newline marking the end of the message arrives.
    while not data.endswith(b"\n"):
        chunk = client_socket.recv(4096)

        # An empty chunk means the connection was closed.
        if not chunk:
            break

        data += chunk

    if not data:
        return None

    # Convert received bytes -> text -> Python dictionary.
    return json.loads(data.decode("utf-8"))


def send_request(query):
    
    # Creates a search request, sends it to the gateway,
    # and waits for the gateway's response.

    # Each request receives a unique ID so that requests and
    # responses can be matched throughout the distributed system.
    request = {
        "request_id": str(uuid.uuid4()),
        "query": query
    }

    # Log the outgoing message for the Milestone 1 logging requirement.
    print(f"[CLIENT] SENT: {request}")

    client_socket = None

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Do not wait forever if the gateway stops responding.
        client_socket.settimeout(5)

        client_socket.connect((HOST, PORT))

        message = json.dumps(request) + "\n"
        client_socket.sendall(message.encode("utf-8"))

        response = receive_message(client_socket)

        if response is None:
            print("[CLIENT] ERROR: No response received from the server.")
            return None

        print(f"[CLIENT] RECEIVED: {response}")

        return response

    except ConnectionRefusedError:
        print(
            f"[CLIENT] ERROR: Could not connect to the main server "
            f"at {HOST}:{PORT}."
        )
        return None

    except socket.timeout:
        print("[CLIENT] ERROR: Server response timed out.")
        return None

    except json.JSONDecodeError:
        print("[CLIENT] ERROR: Server returned an invalid JSON response.")
        return None

    except OSError as error:
        print(f"[CLIENT] NETWORK ERROR: {error}")
        return None

    finally:
        if client_socket is not None:
            client_socket.close()


def display_results(response):
    # Displays the search results returned by the distributed
    # search engine in a readable format.


    if response is None:
        return

    # Display an error returned by the server, if one exists.
    if "error" in response:
        print(f"\nServer Error: {response['error']}")
        return

    # Safely retrieve the list of search results.
    results = response.get("results", [])

    print("\nSearch Results")
    print("--------------")

    if not results:
        print("No matching documents found.")
        return

    # Display each document returned by the search engine.
    for number, result in enumerate(results, start=1):
        title = result.get("title", "Untitled")
        score = result.get("score", 0)
        doc_id = result.get("doc_id", "Unknown")

        print(f"{number}. {title}")
        print(f"   Document ID: {doc_id}")
        print(f"   Score: {score}")


def main():
    # Starts the client and allows the user to perform one search.

    print("==============================")
    print("  Distributed Search Engine")
    print("==============================")

    # Get the search query from the user.
    query = input("Enter a search query: ").strip()

    # Do not send an empty search request.
    if not query:
        print("[CLIENT] ERROR: Search query cannot be empty.")
        return

    # Send the query to the distributed system.
    response = send_request(query)

    # Display the results returned by the gateway.
    display_results(response)


# Only run main() when this file is executed directly.
if __name__ == "__main__":
    main()