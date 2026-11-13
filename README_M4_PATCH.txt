CECS 327 — Milestone 4 client-only update

Copy these relative paths into your existing GitHub repository:
    client/client.py                 (replaces Milestone 3 version)
    tests/test_client_m4.py         (new tests)
    docs/M4_CLIENT_INTEGRATION.md   (teammate contract + demo instructions)

These files do not change your teammates' gateway, index, replica, peer,
or broker code. They remain part of your team's original repository.

From your repo root:
    python3 tests/test_client_m2.py
    python3 tests/test_client_m4.py
    python3 client/client.py --help

The M3 server does not support --coord-status or return Lamport timestamps
until your teammates implement the server-side Milestone 4 pieces.
Existing search/update/status/P2P calls remain compatible.

Not yet a complete Milestone 4 implementation; see integration document.
