# Milestone 3 Video Demo Script — Peer-to-Peer Overlay

**Target length:** ~8–10 minutes  
**Overlay type:** Chord-style DHT (ring + finger tables)  
**Recommended mode:** guided demo with pauses so you can talk between steps

---

## Before you hit record

1. Open a terminal at the **repository root**.
2. Make sure nothing is using ports **9101–9105** (and **8000** if you show the gateway path).
3. Have a second window ready for logs if you want them visible:
   - `logs/peer-9101.log`, `logs/peer-9103.log`, etc.
4. Start the guided demo:

```bash
python3 scripts/p2p_demo.py --pause
```

Press **Enter** only after you finish speaking for that step.

**Backup (manual):** if the guided demo misbehaves, use the commands in each scene below in a fresh terminal.

---

## Opening (≈45 seconds)

**Say:**

> Hi, we're Group [NAME], and this is our Milestone 3 demo for CECS 327.
> We added a peer-to-peer overlay to our distributed search engine.
> The overlay is a **Chord-style DHT**: peers sit on a logical ring, each key hashes to a position on that ring, and the first live peer clockwise owns that key.
> Lookups use finger tables so routing takes about log N hops.
> In this video we'll show: at least four peers, a peer joining, a peer leaving, a lookup routed through the overlay, and a resource discovered through the P2P mechanism.

**On screen:** title slide or repo README / `p2p/peer.py` briefly, then switch to the terminal with `p2p_demo.py --pause` ready.

---

## Scene 1 — Four peers form a ring (≈90 seconds)

**Requirement covered:** at least four peer nodes; basic overlay (ring / Chord)

**Demo does:** starts 9101–9104, then prints the ring.

**Say (before Enter):**

> First we start four peer nodes. Peer 9101 creates the ring. Peers 9102, 9103, and 9104 each join through 9101.
> Each peer gets an ID from a hash of its address, so they land at different positions on a 256-slot ring.

**Press Enter. Wait for the ring table.**

**Say (pointing at output):**

> Here is the ring walked clockwise by following successor pointers. You can see each peer's predecessor and successor, and that the ring closes back to the first peer.
> This is our overlay structure: a Chord ring with finger tables for faster lookup.

**Optional:** open `logs/peer-9103.log` and point at `PEER STARTED`, `JOINED`, `FINGER TABLE`.

---

## Scene 2 — Publish documents into the DHT (≈60 seconds)

**Requirement covered:** data stored / discoverable through peers

**Say (before Enter):**

> Next we publish our document collection into the DHT.
> Every document and every search term becomes a key. Those keys are hashed onto the ring and stored on the peer that owns that range — with a replica on the successor.

**Press Enter.**

**Say:**

> The output shows how many keys were placed and that they are spread across multiple peers. No single peer holds the whole index — discovery has to go through the overlay.

---

## Scene 3 — Lookup routed through the overlay (≈90 seconds)

**Requirement covered:** a lookup request routed through the overlay

**Say (before Enter):**

> Now we issue a lookup for the term "systems", entering the ring at peer 9103 — not necessarily the owner.
> Chord will forward the request using fingers and successors until it reaches the responsible peer. Watch the hop path.

**Press Enter.**

**Say (pointing at path / hops):**

> The path shows which peers the lookup visited, ending at the owner of that key.
> That is a lookup request routed through the overlay — not a direct call to a known shard.

**Optional:** show matching `LOOKUP` / `FORWARD` lines in `logs/peer-9103.log`.

---

## Scene 4 — Resource discovery (≈90 seconds)

**Requirement covered:** a resource discovered through the P2P mechanism

**Say (before Enter):**

> Resource discovery means finding search results and documents by querying peers, not the original Milestone 1 index nodes.
> We'll search for "distributed systems" from peer 9104, then fetch document 3 from peer 9102.

**Press Enter.**

**Say:**

> For each word, the CLI prints the key ID, the hop path, and whether the term was found. Then it merges postings into ranked results.
> Document 3 is discovered at whichever peer owns `doc:3`. The content came from the DHT via peer routing.

---

## Scene 5 — A peer joins (≈90 seconds)

**Requirement covered:** a peer joining the system

**Say (before Enter):**

> Next, peer 9105 joins through 9101. Chord finds its successor, inserts it into the ring, and the successor hands over the keys that now fall in the new peer's range.

**Press Enter. Wait for the updated ring and search.**

**Say:**

> The ring now has five peers. The new peer owns some keys it received on join, and search still works when we enter through 9105.
> That demonstrates dynamic membership — the overlay grows without restarting the whole system.

---

## Scene 6 — A peer leaves (≈90 seconds)

**Requirement covered:** a peer leaving the system

**Say (before Enter):**

> Now peer 9102 leaves gracefully. It hands its keys to its successor and tells its predecessor to point at that successor, so the ring stays connected and no data is lost.

**Press Enter.**

**Say:**

> 9102 is gone from the ring. Search from another peer still returns complete results, which shows leave behavior and key handoff.

---

## Scene 7 — Crash repair (optional but strong, ≈60 seconds)

**Not strictly required, but good evidence of a real overlay.**

**Say (before Enter):**

> As an extra, we kill peer 9103 with no goodbye. Stabilization detects the failure, repairs successor and predecessor pointers, and replicas keep the data available.

**Press Enter.**

**Say:**

> The ring is healthy again with the remaining peers, and search still works. That shows the overlay can recover from an abrupt leave.

---

## Closing (≈30–45 seconds)

**Say:**

> To summarize against the Milestone 3 requirements:
> - We run a **peer node** program and form a **Chord ring** overlay.
> - We showed **join** and **leave**.
> - We showed a **lookup routed through the overlay** with a visible hop path.
> - We showed **resource discovery** — search results and a full document found via peers.
> - We ran **more than four peers** during the demo.
> Thank you.

**Stop recording.** Ctrl+C / let the demo exit; it kills remaining peers in its `finally` block.

---

## Checklist (hit every required item)

| Required demo item                         | Scene |
|--------------------------------------------|-------|
| ≥ 4 peer nodes                             | 1     |
| Peer joining                               | 5     |
| Peer leaving                               | 6     |
| Lookup routed through the overlay          | 3     |
| Resource discovered through P2P            | 4     |
| Overlay structure (ring / Chord)           | 1–3   |

---

## Manual command cheat sheet (if not using `p2p_demo.py`)

Run from the repo root. Use separate terminals for peers if you want live logs.

```bash
# Four peers
python3 p2p/peer.py 9101
python3 p2p/peer.py 9102 --join 9101
python3 p2p/peer.py 9103 --join 9101
python3 p2p/peer.py 9104 --join 9101

# Wait a few seconds, then:
python3 scripts/p2p_cli.py ring 9101
python3 scripts/p2p_cli.py publish 9101
python3 scripts/p2p_cli.py lookup 9103 systems
python3 scripts/p2p_cli.py search 9104 "distributed systems"
python3 scripts/p2p_cli.py get-doc 9102 3

# Join
python3 p2p/peer.py 9105 --join 9101
python3 scripts/p2p_cli.py ring 9101

# Leave
python3 scripts/p2p_cli.py leave 9102
python3 scripts/p2p_cli.py ring 9101
python3 scripts/p2p_cli.py search 9104 "distributed systems"
```

**Optional gateway path** (after peers are up and docs published; start gateway / `run_all` as usual):

```bash
python3 client/client.py --p2p-ring
python3 client/client.py --p2p-search "distributed systems"
```

---

## Recording tips

- Zoom the terminal to ~16–18 pt so hop paths and the ring table are readable.
- Pause 1–2 seconds after each command so the output settles before you talk.
- When you say “hop path,” literally highlight the `path : 9103 -> …` line with the mouse.
- If a join/leave ring looks incomplete, wait 3–5 seconds and re-run `ring 9101` — stabilization may still be catching up.
- Do **not** rely on reading long log dumps aloud; pick one JOIN line and one LOOKUP/FORWARD line.
