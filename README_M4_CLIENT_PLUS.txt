CECS 327 MILESTONE 4 — ENHANCED CLIENT PATCH

This patch REPLACES the previous M4 client patch (do not need to reapply it).
It is based on the user's provided Milestone 3 repository + their previously installed M4 client patch.

INSTALL from your LOCAL repository root (the folder containing client/ and gateway/):
  unzip -o ~/Downloads/CECS327_M4_client_plus_patch.zip -d .

Only the following files are included:
  client/client.py
  tests/test_client_m4.py
  docs/M4_CLIENT_INTEGRATION.md
  docs/M4_CLIENT_CONTRIBUTION.md
  README_M4_CLIENT_PLUS.txt

TEST:
  python3 tests/test_client_m2.py
  python3 tests/test_client_m4.py
  python3 client/client.py --help

LIVE (run 'bash scripts/run_all.sh' in Terminal 1 and keep it running):
  python3 client/client.py --demo-conflict 16 --evidence-file logs/m4_conflict_evidence.json
  python3 client/client.py --status

LATER (only after the group implements M4 leader election + coord_status route):
  python3 client/client.py --observe-leader 60 --evidence-file logs/m4_leader_evidence.json

WARNING: The original M3 gateway does NOT implement leader election or distributed mutex.
Your client can validate ordering and replica agreement now, but leader observation
requires gateway integration. Client evidence is NOT independent proof of mutex safety.

Do not overwrite changed client files by teammates without reviewing diffs.
