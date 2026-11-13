# Milestone 4 — Client contribution and evidence guide

**Component / owner:** Client (User / Request Side), `client/client.py`  
**Assignment:** CECS 327, Milestone 4: Coordination  
**Purpose:** Demonstrate client logical clocks, timestamped communication, competing writes, eventual consistency checks, and *observation* of leader replacement once a server-side election exists.

## Work implemented in the client

| Feature | Files | What it does | Limitations |
|---|---|---|---|
| Lamport clock | `client/client.py` | Thread-safe process-local logical counter; attach `sender_id` + `lamport_ts` to outgoing requests; apply `max(local, incoming)+1` on timestamped responses | M3 gateway does not yet return logical timestamps; server must implement its own clocks |
| Clock and wire trace | `client/client.py` | `CLOCK SEND`, `CLOCK RECEIVE`, and `--show-wire` show actual request/reply messages and causal clock updates | Client logs alone cannot establish clocks for *every* server node |
| Two-client contention | `client/client.py` | `--demo-conflict 16` launches two independent client processes targeting one shared document | Does not grant/release locks itself |
| Stronger post-write verification | `client/client.py` | Checks two valid, distinct update IDs, 3 shard + 3 replica ACKs, replica digest agreement, and final `next_update_id` cursors | Does not independently prove server critical sections never overlap |
| Exportable evidence | `client/client.py` | `--evidence-file` saves real replies/check results in JSON for reports and video script | Evidence must be collected from the actual integrated run |
| Coordination status | `client/client.py` | `--coord-status` displays leader, election term, resource and lock queue when reported by server | Not supported by existing Milestone 3 gateway |
| Replacement observer | `client/client.py` | `--observe-leader 60` observes current and replacement leader IDs, then tests search availability | Requires server election and an existing reachable gateway during failover |
| Extended unit tests | `tests/test_client_m4.py` | Tests clock rules, concurrency, message protocol, rejection of misleading update acknowledgments, correct final status verification, evidence extraction, leader status parsing and failover observation | Unit tests use mock data; full election/mutex must be tested against real M4 services |

## Run and capture evidence

Terminal 1, with your current repository root open:

```bash
bash scripts/run_all.sh
```

Leave Terminal 1 running, open a second terminal, and run:

```bash
python3 tests/test_client_m2.py
python3 tests/test_client_m4.py
python3 client/client.py --search "distributed systems" --show-wire
python3 client/client.py --demo-conflict 16 --evidence-file logs/m4_conflict_evidence.json
python3 client/client.py --status
```

Once the team adds `coord_status` and leader replacement, also run:

```bash
python3 client/client.py --coord-status --show-wire
python3 client/client.py --observe-leader 60 --evidence-file logs/m4_leader_evidence.json
```

While the observer is running, a teammate must perform the actual failure/replacement action *using the agreed server procedure*. Do not kill your only gateway unless a reachable gateway will continue to accept status requests. Capture the actual election logs from the coordinator and actual mutex REQUEST/GRANT/RELEASE logs; include one complete nonoverlapping critical-section trace.

For your written report, quote the real run's observed IDs, digests, and pass/fail checks. Do not copy illustrative sample values into the report as measured results. `logs/*.json` will not be staged by the suggested `git add` command; attach the evidence to your group's report separately if desired.

## Contribution report paragraph (edit after the integrated run)

> Developed and tested the Milestone 4 client-side coordination interface. Implemented a thread-safe Lamport logical clock and added sender identifiers and logical timestamps to JSON requests. Added client commands for coordination status and leader-replacement observation. Extended the concurrent-update demonstration to launch independent client processes, verify ordered update identifiers and replica synchronization using actual gateway acknowledgments, and export machine-readable evidence for the report. Expanded automated tests for clock updates, response validation, conflicting updates, and leader observation. Server-side leader election and distributed mutual exclusion were implemented separately by the gateway/coordinator team and require integrated verification.

**Important distinction:** This paragraph describes code contribution, not proof of all M4 requirements. Only claim integrated server-side behavior when the actual group implementation and logs support it.

## Commands to stage your portion only

```bash
git status
git add client/client.py tests/test_client_m4.py docs/M4_CLIENT_INTEGRATION.md docs/M4_CLIENT_CONTRIBUTION.md
git diff --cached --stat
git commit -m "Milestone 4: Strengthen client coordination demos and evidence"
git push origin main
```

If the group has changed these files since the provided Milestone 3 ZIP, review the differences before extracting/replacing them. Verify no uncommitted teammate changes are overwritten.
